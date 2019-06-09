import atexit
import psycopg2
import requests

api_key_server: str = 'http://localhost:8080/get'
user: str = 'admin'
password: str = ''
pg_host: str = 'localhost'
pg_port: str = '5432'
database: str = 'youtube'


def connect() -> psycopg2:
    print('Connecting to db')
    connection: psycopg2 = psycopg2.connect(
        user=user,
        password=password,
        host=pg_host,
        port=pg_port,
        database=database)

    def exit_func() -> None:
        print('Closing connection')
        conn.close()

    atexit.register(exit_func)
    return connection


conn: psycopg2 = connect()


def get_api_key() -> str:
    return requests.get(api_key_server).text


def main() -> None:
    print(get_api_key())


if __name__ == '__main__':
    main()
