import requests
from config import API_NINJAS_KEY
from datetime import datetime, timedelta
from config import OPEN_WEATHER_KEY

def get_coordinates(city):
    # Input validation for security
    if not city or not isinstance(city, str) or len(city.strip()) == 0:
        print("Error: Invalid city parameter")
        return None, None
    
    # Get coordinates from API Ninjas
    api_lat_long_url = 'https://api.api-ninjas.com/v1/city?name={}'.format(city)
    
    try:
        lat_long_response = requests.get(api_lat_long_url, headers={'X-Api-Key': API_NINJAS_KEY}, timeout=10)
        
        if lat_long_response.status_code == requests.codes.ok:
            city_data = lat_long_response.json()
            if city_data and len(city_data) > 0:
                lat = city_data[0].get('latitude')
                lon = city_data[0].get('longitude')
                return lat, lon
            else:
                print("Error: City not found")
                return None, None
        else:
            print("Error:", lat_long_response.status_code, lat_long_response.text)
            return None, None
            
    except requests.exceptions.RequestException as e:
        print(f"Error getting coordinates: {str(e)}")
        return None, None


def convert_to_local_time(iso_time_str, tz_offset_seconds):
    # Parse manually, dropping the trailing Z
    utc_time = datetime.strptime(iso_time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    
    # Apply offset
    local_time = utc_time + timedelta(seconds=tz_offset_seconds)
    return local_time



def get_timezone_offset(lat_h, lon_h):

    try:
        current_weather_url = 'https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}'.format(
            lat_h, lon_h, OPEN_WEATHER_KEY
        )
        current_weather_response = requests.get(current_weather_url, timeout=10)
        
        if current_weather_response.status_code == requests.codes.ok:
            weather_data = current_weather_response.json()
            timezone_offset = weather_data.get('timezone', 0)  # Offset in seconds from UTC
            return timezone_offset
        else:
            print("Warning: Could not get timezone info, using UTC")
            return 0
    except requests.exceptions.RequestException as e:
        print(f"Warning: Could not get timezone info: {str(e)}, using UTC")
        return 0

