from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URI = os.getenv('DATABASE_URI')
API_KEY=os.getenv('API_KEY')