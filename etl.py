
import requests
from datetime import datetime, timezone
from supabase import create_client, Client
from config import SUPABASE_KEY, SUPABASE_URL
from config import OPEN_WEATHER_KEY, OPEN_UV_KEY
from helper_functions import get_coordinates, get_timezone_offset, convert_to_local_time

def etl():

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    cities = ['san francisco', 'new york']

    for city in cities:


        lat, long = get_coordinates(city)
        tz_offset_seconds = get_timezone_offset(lat, long)

        #ETL Pipeline 
        ##EXTRACT (E)

        #extract air quality data

        api_air_quality_url = "http://api.openweathermap.org/data/2.5/air_pollution/forecast?lat={}&lon={}&appid={}".format(
            lat, long, OPEN_WEATHER_KEY
        )
        response = requests.get(api_air_quality_url)

        if response.status_code == 200:
            print("Success! Air Quality Data")
        else:
            print(f"Failed with status code: {response.status_code}")

        data = response.json()

        aq_data =  [(item["dt"], item["main"]["aqi"]) for item in data["list"]]

        #extract weather data

        api_weather_url = 'https://api.openweathermap.org/data/2.5/forecast?lat={}&lon={}&units=metric&appid={}'.format(
            lat, long, OPEN_WEATHER_KEY
        )
        response = requests.get(api_weather_url)


        if response.status_code == 200:
            print("Success! Weather Data")
        else:
            print(f"Failed with status code: {response.status_code}")

        data = response.json()

        weather_data = [(item["dt"], item["main"]["temp"]) for item in data["list"]]


        #extract UV data


        api_uv_url = 'https://api.openuv.io/api/v1/forecast?lat={}&lng={}'.format(lat, long)


        headers = {
            'x-access-token': OPEN_UV_KEY,
            'Content-Type': 'application/json'
        }

        response = requests.get(api_uv_url, headers=headers, timeout=10)

        if response.status_code == 200:
            print("Success! UV Data")
        else:
            print(f"Failed with status code: {response.status_code}")

        data = response.json()

        uv_data = [(item["uv_time"], item["uv"]) for item in data["result"]]




        ##TRANSFORM(T)

        #transform air quality data

        #convert to local time 

        aq_data_for_postgres = []

        for dt, aqi in aq_data:
            # Convert Unix timestamp → UTC ISO string
            
            utc_iso = datetime.fromtimestamp(dt, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            # Convert to local time
            local_time = convert_to_local_time(utc_iso, tz_offset_seconds)
            
            # Format as string: YYYY-MM-DD HH:MM:SS
            formatted_time = local_time.strftime("%Y-%m-%d %H:%M:%S")
            
            aq_data_for_postgres.append((formatted_time, aqi, city))
            

        #transform weather data

        #convert to local time 

        weather_data_for_postgres = []

        for dt, temp in weather_data:
            # Convert Unix timestamp → UTC ISO string
            utc_iso = datetime.fromtimestamp(dt, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            
            # Convert to local time
            local_time = convert_to_local_time(utc_iso, tz_offset_seconds)
            
            # Format as string: YYYY-MM-DD HH:MM:SS
            formatted_time = local_time.strftime("%Y-%m-%d %H:%M:%S")
            
            weather_data_for_postgres.append((formatted_time,temp, city))
            
        #transform uv data


        #convert to local time 

        uv_data_for_postgres = []

        for dt_str, uv in uv_data:
            # Convert ISO UTC string → local datetime
            local_time = convert_to_local_time(dt_str, tz_offset_seconds)
            
            # Format as string: YYYY-MM-DD HH:MM:SS
            formatted_time = local_time.strftime("%Y-%m-%d %H:%M:%S")
            
            uv_data_for_postgres.append((formatted_time, uv, city))

            
        #LOAD(L)

        #load air quality data 

        rows = [{"timestamp": ts, "aqi": aqi, "city": city} for ts, aqi, city in aq_data_for_postgres]

        response = supabase.table("air_quality_data").insert(rows).execute()

        #load weather data

        rows = [{"timestamp": ts, "temperature": temp, "city": city} for ts, temp, city in weather_data_for_postgres]

        #insert

        response = supabase.table("weather_data").insert(rows).execute()


        #load uv data

        rows = [{"timestamp": ts, "uv": uv, "city": city} for ts, uv, city in uv_data_for_postgres]

        response = supabase.table("uv_data").insert(rows).execute()




if __name__ == "__main__":
    etl()