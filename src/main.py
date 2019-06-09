import requests

api_key_server: str = 'http://localhost:8080/get'


def get_api_key() -> str:
    return requests.get(api_key_server).text


def main() -> None:
    print(get_api_key())


if __name__ == '__main__':
    main()
