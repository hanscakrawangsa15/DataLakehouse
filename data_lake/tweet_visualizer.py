from wordcloud import WordCloud
import matplotlib.pyplot as plt
import pandas as pd
import os


# Load structured .txt file and extract only tweet_text column
base_dir = os.path.dirname(os.path.abspath(__file__))
structured_txt_path = os.path.join(base_dir, "adventureworks", "tweets", "adventureworks_structured_150_tweets.txt")

df = pd.read_csv(structured_txt_path, sep='\t', header=None, names=[
    'tweet_id', 'tweet_text', 'timestamp', 'user_location', 'sentiment', 'matched_product'
])


# Gabungkan semua teks tweet
text_combined = ' '.join(df['tweet_text'].astype(str).tolist())

# Generate WordCloud
wordcloud = WordCloud(
    width=1000,
    height=600,
    background_color='white',
    max_words=200,
    collocations=False
).generate(text_combined)

# Tampilkan WordCloud
plt.figure(figsize=(12, 7))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.title("WordCloud: AdventureWorks Tweets", fontsize=16)
plt.show()
