from bs4 import BeautifulSoup, Tag, XMLParsedAsHTMLWarning
import requests
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

stations = [
    "사당", "방배", "서초",
    "교대", "강남", "역삼", "선릉"
]

directory = "/opt/airflow/plugins"

def get_station_data(datetime: str):
    print('datetime', datetime)
    year, month, _ = datetime.split("-")

    url = "http://openapi.seoul.go.kr:8088/<auth_key>/xml/CardSubwayTime/1/5"

    result = {}
    for station in stations:
        request_url = f"{url}/{year}{month}/2호선/{station}"
        print(request_url)
        html = requests.get(request_url)
        bs = BeautifulSoup(html.content)

        tags: list[Tag] = bs.find_all(lambda tag: tag.name.startswith("hr_"))
        tot = 0
        for tag in tags:
            tot += int(tag.text)
        
        result[station] = tot
        
    with open(f"{directory}/{datetime}.json", "w") as f:
        f.write(result)

