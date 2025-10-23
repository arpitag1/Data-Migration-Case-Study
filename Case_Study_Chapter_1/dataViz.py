from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["local"]
collection = db["clothing"]

# Load data into a DataFrame
data = pd.DataFrame(list(collection.find()))

# Example: Plot a bar chart of counts by category
data['Division Name'].value_counts().plot(kind='bar')
plt.title("Division Name Distribution")
plt.xlabel("Division Name")
plt.ylabel("Count")
plt.show()