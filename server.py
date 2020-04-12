import asyncio
import aiomysql
import aiohttp
import async_timeout
import ast
import json
from datetime import datetime, timedelta
import sshtunnel
import pymysql


class RatesServer:
    current_rates = []
    current_messages = {}
    active_subscribers = {}

    def __init__(self, loop):
        self.loop = loop
        self.port = self.open_tunnel()
        self.rate_names = self.select_rate_names()
        self.rev_rate_names = dict(map(reversed, self.rate_names.items()))

    @staticmethod
    def open_tunnel():
        sshtunnel.SSH_TIMEOUT = 5.0
        sshtunnel.TUNNEL_TIMEOUT = 5.0

        tunnel = sshtunnel.SSHTunnelForwarder(
                ('ssh.pythonanywhere.com'),
                ssh_username='username',
                ssh_password='password',
                remote_bind_address=(
                        'MariaChichkan.mysql.pythonanywhere-services.com', 3306)
        )
        tunnel.start()
        return tunnel.local_bind_port

    async def main(self):
        while True:
            async with aiohttp.ClientSession() as session:
                html = await self.fetch(session, 'https://ratesjson.fxcm.com/DataDisplayer')
                timestamp = datetime.now()
                self.modify_current_rates(html, timestamp)
                await self.update_db()
                await self.send_periodic_messages()

    @staticmethod
    async def fetch(session, url):
        with async_timeout.timeout(10):
            async with session.get(url) as response:
                return await response.text()

    def modify_current_rates(self, html, timestamp):
        start = html.find('[')
        end = html.rfind(']')
        html = html[start: end + 1]
        my_data = ast.literal_eval(html)
        current_rates = []
        current_messages = {}
        for val in my_data:
            if val['Symbol'] in self.rate_names.values():
                current_rates.append((self.rev_rate_names[val['Symbol']], timestamp,
                                           (float(val['Bid']) + float(val['Ask'])) / 2))
                current_messages[self.rev_rate_names[val['Symbol']]] = json.dumps({"message":
                        {"assetName": val['Symbol'], "time": str(timestamp),
                         "assetId": self.rev_rate_names[val['Symbol']],
                         "value": (float(val['Bid']) + float(val['Ask'])) / 2}, "action": "point"})
        self.current_rates = current_rates
        self.current_messages = current_messages

    async def update_db(self):
        conn = await aiomysql.connect(host='127.0.0.1', port=self.port,
                                      user='user', password='password', db='MariaChichkan$Rates',
                                      loop=self.loop)

        cur = await conn.cursor()
        await cur.executemany("INSERT INTO MariaChichkan$Rates.rates_values (id, timestamp, value)"
                              "VALUES  (%s, %s, %s);", self.current_rates)
        await conn.commit()
        await cur.close()
        conn.close()

    async def handle_client(self, reader, writer):
        try:
            while True:
                request = (await reader.read(255)).decode('utf8')
                if request != "":
                    data = json.loads(request)
                    if data['action'] == 'assets':
                        assets = []
                        for key, val in self.rate_names.items():
                            assets.append({'id': key, 'name': val})
                        response = {'action': 'assets', 'message': {'assets': assets}}
                        json_response = json.dumps(response)
                        writer.write(json_response.encode('utf8'))
                        await writer.drain()
                        writer.close()

                    elif data['action'] == 'subscribe':
                        assetid = data['message']['assetId']
                        timestamp = datetime.now() - timedelta(minutes=15)
                        assetid_db_data = await self.select_db(assetid, timestamp)
                        # отправить данные клиенту
                        points = []
                        for asset in assetid_db_data:
                            points.append({"assetName": self.rate_names[asset[0]], "time": str(asset[1]), "assetId": asset[0],
                                           "value": asset[2]})
                        response = {"message": {"points": points}, "action": "asset_history"}
                        json_response = json.dumps(response)
                        writer.write(json_response.encode('utf8'))
                        await writer.drain()
                        remote_host, remote_port = writer.get_extra_info("peername")
                        self.active_subscribers[(remote_host, remote_port)] = (writer, assetid)
                else:
                    break
        except (BrokenPipeError, ConnectionResetError):
            writer.close()

    async def send_periodic_messages(self):
        active_subscribers = self.active_subscribers
        for k, v in active_subscribers.items():
            try:
                if v:
                    writer = v[0]
                    assetid = v[1]
                    writer.write(self.current_messages[assetid].encode('utf8'))
                    await writer.drain()
                    await asyncio.sleep(1)
            except (BrokenPipeError, ConnectionResetError):
                writer.close()
                self.active_subscribers[k] = None

    async def select_db(self, assetid, timestamp):
        conn = await aiomysql.connect(host='127.0.0.1', port=self.port,
                                      user='username', password='password', db='MariaChichkan$Rates',
                                      loop=self.loop)
        sql_query = "SELECT * FROM MariaChichkan$Rates.rates_values WHERE id = %s AND timestamp >= ('%s');" % \
                    (assetid, timestamp)
        cur = await conn.cursor()
        await cur.execute(sql_query)
        assetid_db_data = await cur.fetchall()
        await cur.close()
        conn.close()
        return assetid_db_data

    def select_rate_names(self):
        connection = pymysql.connect(
            host='127.0.0.1',
            port=self.port,
            user='username',
            password=password,
            db='MariaChichkan$Rates',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM MariaChichkan$Rates.rates_names;")
        records = cursor.fetchall()
        rate_names = {r['id']: r['symbol'] for r in records}
        connection.close()
        return rate_names


def run_server(host, port):
    loop = asyncio.get_event_loop()
    server = RatesServer(loop)
    coro = asyncio.start_server(server.handle_client, host, port, loop=loop)
    serv = loop.run_until_complete(asyncio.gather(server.main(), coro))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    serv.close()
    loop.run_until_complete(serv.wait_closed())
    loop.close()


run_server('127.0.0.1', 8080)

