from geopy.geocoders import Nominatim

def get_state_from_coordinates(latitude, longitude):
    try:
        geolocator = Nominatim(user_agent="cubing-mexico", timeout=10)
        location = geolocator.reverse((latitude, longitude), addressdetails=True)
        if location and "address" in location.raw:
            # Return state if available, otherwise default to "Ciudad de México"
            return location.raw["address"].get("state") or "Ciudad de México"
        return None
    except Exception:
        return None

def to_camel_case(snake_str):
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def convert_keys_to_camel_case(data):
    if isinstance(data, dict):
        return {to_camel_case(k): convert_keys_to_camel_case(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_keys_to_camel_case(item) for item in data]
    return data
