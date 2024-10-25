import requests
import time
import os
import logging

# Set up logging
logger = logging.getLogger(__name__)

# Get credentials from environment variables or use defaults
credentials = {
    "username": os.getenv('API_USERNAME', 'Kamlesh123'),
    "password": os.getenv('API_PASSWORD', '1234567')
}

headers = {
    "Content-Type": "application/json"
}

def generate_token(api_url):
    """Generate authentication token from API"""
    try:
        response = requests.post(
            f"{api_url}/get_token", 
            json=credentials, 
            headers=headers,
            timeout=30
        )
        
        if response.status_code == 200:
            token = response.json().get("token")
            return token
        else:
            logger.error(f"Failed to generate token: {response.status_code} - {response.content}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error generating token: {str(e)}")
        return None

def fetch_data_from_api(api_url):
    """Fetch data from API with retries"""
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            token = generate_token(api_url)
            
            if not token:
                logger.error("No token available")
                return None
            
            headers_with_auth = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            response = requests.get(
                f"{api_url}/nallampatti_data", 
                headers=headers_with_auth,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:  # Unauthorized
                logger.warning("Token expired or invalid, retrying...")
                time.sleep(retry_delay)
                continue
            else:
                logger.error(f"Failed to fetch data: {response.status_code} - {response.content}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                return None

    return None