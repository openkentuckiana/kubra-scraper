import mercantile
import polyline
import requests
from dotenv import load_dotenv

from base_scraper import DeltaScraper

load_dotenv()

MIN_ZOOM = 6


class KubraScraper(DeltaScraper):
    base_url = "https://kubra.io/"
    data_url_template = base_url + "{data_path}/public/summary-1/data.json"
    service_areas_url_template = base_url + "{regions}/{regions_key}/serviceareas.json"
    quadkey_url_template = base_url + "{data_path}/public/cluster-3/{quadkey}.json"

    record_key = "id"
    noun = "outage"

    total_downloaded = 0
    total_requests = 0

    @property
    def state_url(self):
        return (
            self.base_url
            + f"stormcenter/api/v1/stormcenters/{self.instance_id}/views/{self.view_id}/currentState?preview=false"
        )

    def __init__(self, github_token):
        super().__init__(github_token)
        state = self._make_request(self.state_url).json()
        regions_key = list(state["datastatic"])[0]
        regions = state["datastatic"][regions_key]
        self.service_areas_url = self.service_areas_url_template.format(regions=regions, regions_key=regions_key)
        self.data_path = state["data"]["interval_generation_data"]

    @staticmethod
    def _get_bounding_box(points):
        x_coordinates, y_coordinates = zip(*points)
        return [min(y_coordinates), min(x_coordinates), max(y_coordinates), max(x_coordinates)]

    @staticmethod
    def _get_neighboring_quadkeys(quadkey):
        tile = mercantile.quadkey_to_tile(quadkey)
        return [
            mercantile.quadkey(mercantile.Tile(x=tile.x, y=tile.y - 1, z=tile.z)),  # N
            mercantile.quadkey(mercantile.Tile(x=tile.x + 1, y=tile.y, z=tile.z)),  # E
            mercantile.quadkey(mercantile.Tile(x=tile.x, y=tile.y + 1, z=tile.z)),  # S
            mercantile.quadkey(mercantile.Tile(x=tile.x - 1, y=tile.y, z=tile.z)),  # W
        ]

    def _get_service_area_quadkeys(self):
        """Get the quadkeys for the entire service area"""
        res = self._make_request(self.service_areas_url).json()
        areas = res.get("file_data")[0].get("geom").get("a")

        points = []
        for geom in areas:
            # Geometries are in Google's Polyline Algorithm format
            # https://developers.google.com/maps/documentation/utilities/polylinealgorithm
            points += polyline.decode(geom)

        bbox = self._get_bounding_box(points)

        return [mercantile.quadkey(t) for t in mercantile.tiles(*bbox, zooms=[MIN_ZOOM])]

    def _get_quadkey_for_point(self, point, zoom):
        ll = polyline.decode(point)[0]
        return mercantile.quadkey(mercantile.tile(lng=ll[1], lat=ll[0], zoom=zoom))

    def fetch_data(self):
        data = self._make_request(self.data_url_template.format(data_path=self.data_path)).json()
        expected_outages = data["summaryFileData"]["totals"][0]["total_outages"]

        quadkeys = self._get_service_area_quadkeys()

        outages = self._fetch_data(quadkeys, set()).values()
        number_out = sum([o["numberOut"] for o in outages])

        print(f"Made {self.total_requests} requests, fetching {self.total_downloaded/1000} KB.")

        if number_out != expected_outages:
            raise Exception(f"Outages found ({number_out}) does not match expected outages ({expected_outages})")

        return list(outages)

    def _fetch_data(self, quadkeys, already_seen, zoom=MIN_ZOOM, cluster_search=False):
        outages = {}

        for q in quadkeys:
            url = self.quadkey_url_template.format(data_path=self.data_path, quadkey=q)
            if url in already_seen:
                continue
            already_seen.add(url)
            res = self._make_request(url)

            # If there are no outages in the area, there won't be a file.
            if not res.ok:
                continue

            for o in res.json()["file_data"]:
                if o["desc"]["cluster"]:
                    # If it's a cluster, we need to drill down (zoom in)
                    outages.update(self._fetch_data([self._get_quadkey_for_point(o["geom"]["p"][0], zoom + 1)], already_seen, zoom + 1, True))
                else:
                    # If we were drilling down, once we get to the outage, we need to look at neighboring quadkeys in case
                    # any outages that were in the cluster spanned a quadkey boundary.
                    if cluster_search:
                        outages.update(self._fetch_data(self._get_neighboring_quadkeys(q), already_seen, zoom))

                    outage_info = self._get_outage_info(o, url)
                    outages[outage_info["id"]] = outage_info

        return outages

    def display_record(self, outage):
        display = [f"  {outage['custAffected']} outage(s) added with {outage['custAffected']} customers affected"]
        return "\n".join(display)

    @staticmethod
    def _get_outage_info(raw_outage, url):
        desc = raw_outage["desc"]
        loc = polyline.decode(raw_outage["geom"]["p"][0])

        return {
            "id": desc["inc_id"],
            "etr": desc["etr"],
            "etrConfidence": desc["etr_confidence"],
            "comments": desc["comments"],
            "cause": desc["cause"]["EN-US"] if desc["cause"] else None,
            "numberOut": desc["n_out"],
            "custAffected": desc["cust_a"]["val"],
            "crewStatus": desc["crew_status"],
            "startTime": desc["start_time"],
            "latitude": loc[0][0],
            "longitude": loc[0][1],
            "source": url,
        }

    def _make_request(self, url):
        res = requests.get(url)
        self.total_downloaded += len(res.content)
        self.total_requests += 1
        return res
