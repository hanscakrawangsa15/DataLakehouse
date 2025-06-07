import tkinter as tk
from tkinter import filedialog
from wordcloud import WordCloud
import matplotlib.pyplot as plt

def generate_wordcloud_from_txt(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        text = file.read()

    wordcloud = WordCloud(
        width=800,
        height=600,
        background_color='white',
        max_words=200,
        collocations=False
    ).generate(text)

    plt.figure(figsize=(10, 6))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.title("WordCloud - AdventureWorks Tweets", fontsize=16)
    plt.show()

def open_file_and_generate():
    file_path = filedialog.askopenfilename(filetypes=[("Text files", "*.txt")])
    if file_path:
        generate_wordcloud_from_txt(file_path)

# Setup GUI
root = tk.Tk()
root.title("AdventureWorks WordCloud Generator")
root.geometry("400x200")

label = tk.Label(root, text="Click the button below to select a .txt file and generate a WordCloud", wraplength=350)
label.pack(pady=20)

btn = tk.Button(root, text="Select .txt File", command=open_file_and_generate)
btn.pack()

root.mainloop()