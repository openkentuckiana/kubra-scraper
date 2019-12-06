import mercantile
import polyline
import requests
from dotenv import load_dotenv

from base_scraper import DeltaScraper

load_dotenv()

MIN_ZOOM = 6


class KubraScraper(DeltaScraper):
    base_url = "https://kubra.io/"
    service_areas_url_template = base_url + "{regions}/{regions_key}/serviceareas.json"
    quadkey_url_template = base_url + "{data_path}/public/cluster-3/{quadkey}.json"

    record_key = "id"
    noun = "outage"

    @property
    def state_url(self):
        return (
            self.base_url
            + f"stormcenter/api/v1/stormcenters/{self.instance_id}/views/{self.view_id}/currentState?preview=false"
        )

    def __init__(self, github_token):
        super().__init__(github_token)
        state = requests.get(self.state_url).json()
        regions_key = list(state["datastatic"])[0]
        regions = state["datastatic"][regions_key]
        self.service_areas_url = self.service_areas_url_template.format(regions=regions, regions_key=regions_key)
        self.data_path = state["data"]["interval_generation_data"]

    @staticmethod
    def _get_bounding_box(points):
        x_coordinates, y_coordinates = zip(*points)
        return [min(y_coordinates), min(x_coordinates), max(y_coordinates), max(x_coordinates)]

    def _get_service_area_quadkeys(self):
        """Get the quadkeys for the entire service area"""
        res = requests.get(self.service_areas_url).json()
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
        return [mercantile.tile(lng=ll[0], lat=ll[1], zoom=zoom)]

    def fetch_data(self):
        quadkeys = self._get_service_area_quadkeys()
        return self._fetch_data(quadkeys)

    def _fetch_data(self, quadkeys, zoom=MIN_ZOOM):
        outages = []

        for q in quadkeys:
            res = requests.get(self.quadkey_url_template.format(data_path=self.data_path, quadkey=q),)

            # If there are no outages in the area, there won't be a file.
            if not res.ok:
                continue

            for o in res.json()["file_data"]:
                if o["desc"]["cluster"]:
                    # We need to zoom in on clusters to get individual events.
                    outages.extend(self._fetch_data(self._get_quadkey_for_point(o["geom"]["p"][0], zoom + 1)))
                else:
                    outages.append(self._get_outage_info(o))

        return outages

    def display_record(self, outage):
        display = [f"  {outage['custAffected']} outage(s) added with {outage['custAffected']} customers affected"]
        return "\n".join(display)

    @staticmethod
    def _get_outage_info(raw_outage):
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
        }
