import streamlit as st
import pandas as pd
import plotly.express as px

# Titre
st.title("🌤️ Kayak Trip Planner")
st.subheader("Trouvez votre prochaine destination en France")

# Loading
df_weather = pd.read_csv("weather_raw.csv")
df_hotels = pd.read_csv("hotels_raw.csv")
df_cities = pd.read_csv("cities.csv")

# Scoring
df_score = df_weather.groupby("city").agg(
    avg_temp_max=("temp_max", "mean"),
    avg_humidity=("humidity", "mean"),
    avg_pop=("pop", "mean")
).reset_index()

# Sliders to choose the importance of the criteria
st.sidebar.title("Choose your preferred criteria")
weather_temp = st.sidebar.slider("Temperature", 0, 10, 5)
weather_rain = st.sidebar.slider("No rain", 0, 10, 5)
weather_humidity = st.sidebar.slider("No humidity", 0, 10, 5)

# Weighted Score
df_score["score"] = (
    df_score["avg_temp_max"].rank() * weather_temp +
    df_score["avg_pop"].rank(ascending=False) * weather_rain +
    df_score["avg_humidity"].rank(ascending=False) * weather_humidity
)

# Top 5
top_5 = df_score.sort_values("score", ascending=False).head(5)
top_5_cities = top_5["city"].tolist()

# Carte 1 : Top 5 destinations
st.header("Top 5 destinations")
df_map = df_cities[df_cities["city"].isin(top_5_cities)].merge(top_5[["city", "score"]], on="city")
fig1 = px.scatter_mapbox(
    df_map,
    lat="latitude",
    lon="longitude",
    hover_name="city",
    size="score",
    color="score",
    zoom=4,
)
fig1.update_layout(mapbox_style="open-street-map")
st.plotly_chart(fig1)

# Carte 2 : Top 20 hotels
st.header("Top 20 hotels")
df_top_20 = df_hotels[df_hotels["city"].isin(top_5_cities)].copy()
df_top_20["score"] = pd.to_numeric(df_top_20["score"], errors="coerce")
df_top_20 = df_top_20.sort_values("score", ascending=False).head(20)

fig2 = px.scatter_mapbox(
    df_top_20,
    lat="latitude",
    lon="longitude",
    hover_name="name",
    hover_data={"score": True, "city": True},
    size="score",
    color="score",
    zoom=4,
)
fig2.update_layout(mapbox_style="open-street-map")
st.plotly_chart(fig2)