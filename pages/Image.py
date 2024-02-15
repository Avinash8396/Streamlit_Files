import streamlit as st
from PIL import Image

image = Image.open('C:\code\folder\.venv\static\Wallpaper.png')

st.image(image, caption='Sunrise by the mountains')