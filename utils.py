from geopy.geocoders import Nominatim

def get_state_from_coordinates(latitude, longitude):
    try:
        geolocator = Nominatim(user_agent="my_geocoder")
        location = geolocator.reverse((latitude, longitude), addressdetails=True)
        if location and "address" in location.raw:
            # Return state if available, otherwise default to "Ciudad de México"
            return location.raw["address"].get("state") or "Ciudad de México"
        return None
    except Exception:
        return None