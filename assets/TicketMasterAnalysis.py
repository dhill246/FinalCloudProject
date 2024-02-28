import pandas as pd
import numpy as np
import logging
import io
import boto3
import time
import urllib
import re
import s3fs
import folium
from folium.plugins import MarkerCluster
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO

s3 = boto3.client('s3')
bucket ='finalprojectstack-finaldataca7db91c-jwd9cyzim57x' 
# source = "s3://finalprojectstack-finaldataca7db91c-jwd9cyzim57x/ticketmaster.csv"
source = "ticketmaster_og.csv"
df= pd.read_csv(source)

#map 
def color_by_category(category):
    category_colors = {
        'Basketball': 'orange',
        'Rock': 'black',
        'Motorsports/Racing': 'beige',
        'Hockey': 'blue',
        'Baseball': 'brown',
        'Ice Shows': 'cyan',
        'Fairs & Festivals': 'green',
        'Country': 'red',
        'Pop': 'pink',
        'Theatre': 'purple',
        'Golf': 'lime',
        'Football': 'maroon',
        'Performance Art': 'navy',
        'R&B': 'olive',
        'Miscellaneous': 'gray'
    }
    return category_colors.get(category, 'gray')



US_COORDINATES = (37.0902, -95.7129)

map_us = folium.Map(location=US_COORDINATES, zoom_start=5, tiles="cartodb positron")

# Create a MarkerCluster object
marker_cluster = MarkerCluster().add_to(map_us)

# Assuming 'df' is your DataFrame
for index, row in df.iterrows():
    category_color = color_by_category(row['Category'])
    popup_text = f"{row['Name']}: {row['Category']}, Date: {row['Date']}, Time: {row['Time']}"
    folium.CircleMarker(
        location=(row['Latitude'], row['Longitude']),
        radius=5,
        color=category_color,
        fill=True,
        fill_color=category_color,
        fill_opacity=0.7,
        popup=folium.Popup(popup_text, parse_html=True)
    ).add_to(marker_cluster)  # Add to the marker cluster instead of directly to the map

temp_map_path = '/tmp/temp_map.html'
map_us.save(temp_map_path)

# Upload the temporary file to S3
s3 = boto3.client('s3')
bucket_name = 'finalprojectstack-finaldataca7db91c-jwd9cyzim57x'
s3_file_key = 'your_map.html'

# Using upload_file which is meant for file paths
# s3.upload_file(temp_map_path, bucket_name, s3_file_key)

#process
#df = df.drop(columns=["Unnamed: 0"])
df["Date"] = pd.to_datetime(df["Date"])
df["Weekday"] = df["Date"].dt.day_name()
df = df.dropna(subset=["Max"])

#Basketball average min per day
basketball = df[df["Category"] == "Basketball"]
top_5_cities = basketball["City"].value_counts().head(5).index.tolist()
grouped_bball = basketball.groupby(["Weekday", "City"])["Min"].mean().reset_index()

plt.figure(figsize=(12, 7))
for category in top_5_cities:
    category_df = grouped_bball[grouped_bball['City'] == category]
    # Make sure days of the week are in order
    category_df['DayOfWeek'] = pd.Categorical(category_df['Weekday'], categories=
                                             ["Monday", "Tuesday", "Wednesday", "Thursday", 
                                              "Friday", "Saturday", "Sunday"], 
                                             ordered=True)
    category_df = category_df.sort_values('DayOfWeek')
    plt.plot(category_df['Weekday'], category_df['Min'], label=category, linewidth=1, marker="o", markersize=5)

plt.xlabel('Day of the Week')
plt.ylabel('Min Price ($)')
plt.title('Average Minimum Priced Days to Watch Basketball by City')
plt.xticks(rotation=45)  # Rotate day names for better readability
plt.legend()
plt.legend(loc='upper left', bbox_to_anchor=(1, 1))

plt.savefig('pictures/bball_min.png')
# plot_buffer = BytesIO()
# plt.savefig(plot_buffer, format='png')
# plot_buffer.seek(0)
# s3.upload_fileobj(plot_buffer,'finalprojectstack-finaldataca7db91c-jwd9cyzim57x', 'bball_min.png')


# After uploading, you can safely close the plot and buffer
# plt.close()
# plot_buffer.close()

#basketball max by day
basketball = df[df["Category"] == "Basketball"]
top_5_cities = basketball["City"].value_counts().head(5).index.tolist()
grouped_bball = basketball.groupby(["Weekday", "City"])["Max"].mean().reset_index()

plt.figure(figsize=(12, 7))
for category in top_5_cities:
    category_df = grouped_bball[grouped_bball['City'] == category]
    # Make sure days of the week are in order
    category_df['DayOfWeek'] = pd.Categorical(category_df['Weekday'], categories=
                                             ["Monday", "Tuesday", "Wednesday", "Thursday", 
                                              "Friday", "Saturday", "Sunday"], 
                                             ordered=True)
    category_df = category_df.sort_values('DayOfWeek')
    plt.plot(category_df['Weekday'], category_df['Max'], label=category, linewidth=1, marker="o", markersize=5)

plt.xlabel('Day of the Week')
plt.ylabel('Max Price ($)')
plt.title('Average Maximum Price Days to Watch Basketball by City')
plt.xticks(rotation=45)  # Rotate day names for better readability
plt.legend()
plt.legend(loc='upper left', bbox_to_anchor=(1, 1))

plt.subplots_adjust(right=0.85)

plt.savefig('pictures/bball_max.png')
# plot_buffer = BytesIO()
# plt.savefig(plot_buffer, format='png')
# plot_buffer.seek(0)
# s3.upload_fileobj(plot_buffer,'finalprojectstack-finaldataca7db91c-jwd9cyzim57x', 'bball_max.png')


# # After uploading, you can safely close the plot and buffer
# plt.close()
# plot_buffer.close()


#baseball min by day
baseball = df[df["Category"] == "Baseball"]
top_5_cities = baseball["City"].value_counts().head(5).index.tolist()
grouped_baseball = baseball.groupby(["Weekday", "City"])["Min"].mean().reset_index()

plt.figure(figsize=(12, 7))
for category in top_5_cities:
    category_df = grouped_baseball[grouped_baseball['City'] == category]
    # Make sure days of the week are in order
    category_df['DayOfWeek'] = pd.Categorical(category_df['Weekday'], categories=
                                             ["Monday", "Tuesday", "Wednesday", "Thursday", 
                                              "Friday", "Saturday", "Sunday"], 
                                             ordered=True)
    category_df = category_df.sort_values('DayOfWeek')
    plt.plot(category_df['Weekday'], category_df['Min'], label=category, linewidth=1, marker="o", markersize=5)

plt.xlabel('Day of the Week')
plt.ylabel('Value')
plt.title('Average Minimum Price Days to Watch Baseball by City')
plt.xticks(rotation=45)  # Rotate day names for better readability
plt.legend()
plt.legend(loc='upper left', bbox_to_anchor=(1, 1))

plt.savefig('pictures/baseball_min.png')
# plot_buffer = BytesIO()
# plt.savefig(plot_buffer, format='png')
# plot_buffer.seek(0)
# s3.upload_fileobj(plot_buffer,'finalprojectstack-finaldataca7db91c-jwd9cyzim57x', 'baseball_min.png')


# # After uploading, you can safely close the plot and buffer
# plt.close()
# plot_buffer.close()


#baseball max by day

baseball = df[df["Category"] == "Baseball"]
top_5_cities = baseball["City"].value_counts().head(5).index.tolist()
grouped_baseball = baseball.groupby(["Weekday", "City"])["Max"].mean().reset_index()

plt.figure(figsize=(12, 7))
for category in top_5_cities:
    category_df = grouped_baseball[grouped_baseball['City'] == category]
    # Make sure days of the week are in order
    category_df['DayOfWeek'] = pd.Categorical(category_df['Weekday'], categories=
                                             ["Monday", "Tuesday", "Wednesday", "Thursday", 
                                              "Friday", "Saturday", "Sunday"], 
                                             ordered=True)
    category_df = category_df.sort_values('DayOfWeek')
    plt.plot(category_df['Weekday'], category_df['Max'], label=category, linewidth=1, marker="o", markersize=5)

plt.xlabel('Day of the Week')
plt.ylabel('Value')
plt.title('Average Maximum Price Days to Watch Baseball by City')
plt.xticks(rotation=45)  # Rotate day names for better readability
plt.legend()
plt.legend(loc='upper left', bbox_to_anchor=(1, 1))

plt.savefig('pictures/baseball_max.png')
# plot_buffer = BytesIO()
# plt.savefig(plot_buffer, format='png')
# plot_buffer.seek(0)
# s3.upload_fileobj(plot_buffer,'finalprojectstack-finaldataca7db91c-jwd9cyzim57x', 'baseball_max.png')


# # After uploading, you can safely close the plot and buffer
# plt.close()
# plot_buffer.close()

#overall min/max by day
average_min_price_by_day = df.groupby('Weekday')['Min'].mean().reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
average_max_price_by_day = df.groupby('Weekday')['Max'].mean().reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])

average_prices_by_day = pd.DataFrame({
    'Average Min Price': average_min_price_by_day,
    'Average Max Price': average_max_price_by_day
})

fig, ax1 = plt.subplots(figsize=(10, 6))

color_min = 'tab:blue'
ax1.set_xlabel('Day of the Week')
ax1.set_ylabel('Average Price', color=color_min)
min_line, = ax1.plot(average_prices_by_day.index, average_prices_by_day['Average Min Price'], color=color_min, marker='o', linestyle='-', linewidth=2.5, label='Average Min Price')
ax1.tick_params(axis='y', labelcolor=color_min)
ax1.set_xticks(range(len(average_prices_by_day.index)))
ax1.set_xticklabels(average_prices_by_day.index, rotation=45)

ax2 = ax1.twinx()
color_max = 'tab:red'
ax2.set_ylabel('Average Max Price', color=color_max)
max_line, = ax2.plot(average_prices_by_day.index, average_prices_by_day['Average Max Price'], color=color_max, marker='o', linestyle='-', linewidth=2.5, label='Average Max Price')
ax2.tick_params(axis='y', labelcolor=color_max)

fig.tight_layout()


plt.legend([min_line, max_line], ['Average Min Price', 'Average Max Price'], loc='upper left')

plt.title('Average Min and Max Price by Day of the Week')
plt.subplots_adjust(top=0.85)

plt.savefig('pictures/overall.png')

# plot_buffer = BytesIO()
# plt.savefig(plot_buffer, format='png')
# plot_buffer.seek(0)
# s3.upload_fileobj(plot_buffer,'finalprojectstack-finaldataca7db91c-jwd9cyzim57x', 'overall.png')


# # After uploading, you can safely close the plot and buffer
# plt.close()
# plot_buffer.close()

#max price by day of week and category
top_categories = df['Category'].value_counts().head(7).index

# Filter the DataFrame to only include rows with the top 7 categories
top_df = df[df['Category'].isin(top_categories)]

df_filtered = top_df[top_df['Category']!= 'Basketball']
days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

df_filtered['Weekday'] = pd.Categorical(df_filtered['Weekday'], categories=days_order, ordered=True)

# Define a color for each category
category_colors = {
    'Baseball': '#1f77b4',  # blue
    'Hockey': '#a18405',   # orange
    'Theatre': '#2ca02c',  # green
    'Ice Shows': '#d62728',  # red
    'Rock': '#9467bd',     # purple
    'Country': '#8c564b'   # brown
}

# Plot
plt.figure(figsize=(12, 7))

sns.lineplot(
    x='Weekday',
    y='Max',
    hue='Category',
    data=df_filtered,
    ci=None,
    estimator=np.mean,
    style='Category',
    markers=True,
    dashes=False,
    palette=category_colors  # Use the custom palette
)

plt.title('Average Max Price by Day of Week and Category')
plt.ylabel('Average Max')
plt.xlabel('Day of Week')

# Move the legend to the side
plt.legend(title='Category', bbox_to_anchor=(1.05, 1), loc='upper left')

plt.tight_layout()  # Adjust the layout to make room for the legend

plt.savefig('pictures/maxbyday.png')

# plot_buffer = BytesIO()
# plt.savefig(plot_buffer, format='png')
# plot_buffer.seek(0)
# s3.upload_fileobj(plot_buffer,'finalprojectstack-finaldataca7db91c-jwd9cyzim57x', 'maxbyday.png')


# # After uploading, you can safely close the plot and buffer
# plt.close()
# plot_buffer.close()


#max price with grey
unique_categories = top_df['Category'].unique()

# Create color mapping: all categories grey, 'Basketball' orange
color_mapping = {category: 'grey' for category in unique_categories}
color_mapping['Basketball'] = 'orange'

# Plot
plt.figure(figsize=(12, 7))

# Using the 'palette' parameter to specify colors
sns.lineplot(x='Weekday', y='Max', hue='Category', data=top_df, ci=None, estimator=np.mean,
             style='Category', markers=True, dashes=False, palette=color_mapping)

plt.title('Average Max Price by Day of Week and Category')
plt.ylabel('Average Max')
plt.xlabel('Day of Week')

# Move the legend to the side
plt.legend(title='Category', bbox_to_anchor=(1.05, 1), loc='upper left')

plt.tight_layout()  # Adjust the layout to make room for the legend

plt.savefig('pictures/maxbydaygrey.png')


# plot_buffer = BytesIO()
# plt.savefig(plot_buffer, format='png')
# plot_buffer.seek(0)
# s3.upload_fileobj(plot_buffer,'finalprojectstack-finaldataca7db91c-jwd9cyzim57x', 'maxbydaygrey.png')


# # After uploading, you can safely close the plot and buffer
# plt.close()
# plot_buffer.close()


#no basketball
df_filtered = top_df[top_df['Category']!= 'Basketball']
days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

df_filtered['Weekday'] = pd.Categorical(df_filtered['Weekday'], categories=days_order, ordered=True)

# Define a color for each category
category_colors = {
    'Baseball': '#1f77b4',  # blue
    'Hockey': '#a18405',   # orange
    'Theatre': '#2ca02c',  # green
    'Ice Shows': '#d62728',  # red
    'Rock': '#9467bd',     # purple
    'Country': '#8c564b'   # brown
}

# Plot
plt.figure(figsize=(12, 7))

sns.lineplot(
    x='Weekday',
    y='Max',
    hue='Category',
    data=df_filtered,
    ci=None,
    estimator=np.mean,
    style='Category',
    markers=True,
    dashes=False,
    palette=category_colors  # Use the custom palette
)

plt.title('Average Max Price by Day of Week and Category')
plt.ylabel('Average Max')
plt.xlabel('Day of Week')

# Move the legend to the side
plt.legend(title='Category', bbox_to_anchor=(1.05, 1), loc='upper left')

plt.tight_layout()  # Adjust the layout to make room for the legend
plt.show()

plt.savefig('pictures/nobball.png')

# plot_buffer = BytesIO()
# plt.savefig(plot_buffer, format='png')
# plot_buffer.seek(0)
# s3.upload_fileobj(plot_buffer,'finalprojectstack-finaldataca7db91c-jwd9cyzim57x', 'nobball.png')


# # After uploading, you can safely close the plot and buffer
# plt.close()
# plot_buffer.close()

#add bball

unique_categories = top_df['Category'].unique()

# Create color mapping: all categories grey, 'Basketball' orange
color_mapping = {category: 'grey' for category in unique_categories}
color_mapping['Basketball'] = 'orange'

# Plot
plt.figure(figsize=(12, 7))

# Using the 'palette' parameter to specify colors
sns.lineplot(x='Weekday', y='Max', hue='Category', data=top_df, ci=None, estimator=np.mean,
             style='Category', markers=True, dashes=False, palette=color_mapping)

plt.title('Average Max Price by Day of Week and Category')
plt.ylabel('Average Max')
plt.xlabel('Day of Week')

# Move the legend to the side
plt.legend(title='Category', bbox_to_anchor=(1.05, 1), loc='upper left')

plt.tight_layout()  # Adjust the layout to make room for the legend
plt.show()

plt.savefig('pictures/wbball.png')

# plot_buffer = BytesIO()
# plt.savefig(plot_buffer, format='png')
# plot_buffer.seek(0)
# s3.upload_fileobj(plot_buffer,'finalprojectstack-finaldataca7db91c-jwd9cyzim57x', 'wbball.png')


# # After uploading, you can safely close the plot and buffer
# plt.close()
# plot_buffer.close()

#variance
category_colors = {
    'Baseball': '#1f77b4',  # blue
    'Hockey': '#a18405',   # yellow
    'Theatre': '#2ca02c',  # green
    'Ice Shows': '#d62728',  # red
    'Rock': '#9467bd',     # purple
    'Country': '#8c564b',   # brown
    'Basketball': 'orange'
}

maxes = top_df[["Category","Max"]].groupby(by=["Category"]).agg(["mean","std","count"]).reset_index()
maxes.columns = ["Category","max_mean","max_std","max_count"]
maxes['CV'] = maxes['max_std']/maxes['max_mean']
maxes2 = maxes[maxes['max_mean'].notna()]
maxes2 = maxes2.sort_values(by='CV', ascending=False)

plt.figure(figsize=(12, 7))
bars = plt.bar(maxes2['Category'], maxes2['CV'], color=[category_colors[cat] for cat in maxes2['Category']])

plt.title('Variance in Max Price by Category')
plt.xlabel('Category')
plt.ylabel('Max Price Variance')
plt.xticks(rotation=45)  # Rotate category names for better readability

plt.savefig('pictures/variance.png')

# plot_buffer = BytesIO()
# plt.savefig(plot_buffer, format='png')
# plot_buffer.seek(0)
# s3.upload_fileobj(plot_buffer,'finalprojectstack-finaldataca7db91c-jwd9cyzim57x', 'variance.png')


# # After uploading, you can safely close the plot and buffer
# plt.close()
# plot_buffer.close()