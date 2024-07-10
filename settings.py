from pydantic import BaseModel
from dotenv import load_dotenv
import os

load_dotenv()


class Passes(BaseModel):
    url: str = os.getenv('URL')
    key: str = os.getenv('KEY')
    sid: str = os.getenv('ID')


passes_instance = Passes()


