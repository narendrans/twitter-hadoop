'''
@Author: Naren
@Date: 4/21/2014

This script will 

- remove if a word contains special characters
- remove if its a stop word
- remove if its length is less than 2 characters
- Sort the list in descending order
- Prints the top 10 most occuring words
- Generates the charts for @, # and trending words and saves them as images
- Generates a pdf and shows it

The input file should be of the below format

abc 100
allsd 23
ksdll 5

i.e, <word> <word_count>

Use the following commands to install the dependency modules

pip install numpy
pip install matplotlib
pip install scipy

Tested to work in OS X 10.9 with python 2.7, should be compatible with any linux system having python 2.6+

Report bugs to ns75@buffalo.edu
 
'''
 
from itertools import cycle, islice
import sys, operator, os
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np


def main():
    if len(sys.argv) < 2:
        print 'This script will remove unnecessary special characters from wordcount output file and store the result in a new file.'
        print 'Usage: ' + sys.argv[0] + ' <path to input file>'
        print 'ex: ' + sys.argv[0] + ' input.txt'
        return
    
    print 'Please wait while processing data...'
    
    # Dictionaries to hold various counts
    trends = {}
    users = {}
    hashtags = {}
    
    # Files to store the various counts
    trendsFile = 'trendsFile.txt'  # using sys.argv to maintain compatibility between prev versions - Naren
    trendsPng = 'trends.png'
    userFile = 'usersFile.txt'
    
    # Images that shows chart for various counts
    usersPng = 'users.png'
    hashtagFile = 'hashtagFile.txt'
    hashtagPng = 'hashtag.png'

    # Clean up
    os.system('rm -rf trendsFile.txt trends.png usersFile.txt users.png hashtagFile.txt hashtag.png report.pdf')

    # List of stop words
    stopwords = ["a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours    ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"]

    # Open the input and store hashtags, usermentions and filtered words
    with open(sys.argv[1]) as fp:
        for line in fp:
            word = line.split()[0]
            wordCount = line.split()[1]
            
            if word.isalpha():
                trends.update({word:wordCount})
            if word[0] is '@' and len(word) > 3:
                users.update({word:wordCount})
            if word[0] is '#' and len(word) > 3:
                hashtags.update({word:wordCount})

    # Sort the trending words and save to file
    print 'Sorting trending words...'
    sorted_x = sorted(trends.iteritems(), key=lambda trends: int(trends[1]), reverse=True)
    for key, value in sorted_x:
        with open(trendsFile, "a") as myfile:
                if not (key.lower() in stopwords) and key.__len__() > 2:
                    myfile.write(key + " " + value + "\n")

    # Sort the user mentions and save to file
    print 'Sorting user mentions...'
    sorted_x = sorted(users.iteritems(), key=lambda users: int(users[1]), reverse=True)
    for key, value in sorted_x:
        with open(userFile, "a") as myfile:
                    myfile.write(key + " " + value + "\n")

    # Sort the hashtags and save to file
    print 'Sorting hashtags...'
    sorted_x = sorted(hashtags.iteritems(), key=lambda hashtags: int(hashtags[1]), reverse=True)
    for key, value in sorted_x:
        with open(hashtagFile, "a") as myfile:
                    myfile.write(key + " " + value + "\n")

    # END of processing

    # Print each of segments to console
                    
    count = 0
    key = []
    value = []

    print '\n------------------------------------------------------'
    print "List of top 10 words:"
    print '------------------------------------------------------'
    with open(trendsFile) as fp:
        for line in fp:
            sys.stdout.write(line)
            key.append(int(line.split()[1]))
            value.append(line.split()[0])
            count = count + 1
            if count == 10:
                break

    count1 = 0
    key1 = []
    value1 = []

    print '\n------------------------------------------------------'
    print "List of top 10 user mentions:"
    print '------------------------------------------------------'
    with open(userFile) as fp:
        for line in fp:
            sys.stdout.write(line)
            key1.append(int(line.split()[1]))
            value1.append(line.split()[0])
            count1 = count1 + 1
            if count1 == 10:
                break


    count2 = 0
    key2 = []
    value2 = []

    print '\n------------------------------------------------------'
    print "List of top 10 hashtags:"
    print '------------------------------------------------------'
    with open(hashtagFile) as fp:
        for line in fp:
            sys.stdout.write(line)
            key2.append(int(line.split()[1]))
            value2.append(line.split()[0])
            count2 = count2 + 1
            if count2 == 10:
                break

    # END of printing to console

    print '\nPlease wait while generating the charts...'

    # Chart generation begins

    N = len(key)

    ind = np.arange(N)  # the trends locations for the groups
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    my_colors = list(islice(cycle(['#0033CC', '#00FFCC', '#FF0066', '#FF9900', '#009933', '#660066', '#663300', '#003300', '#666633', '#3333CC']), None, N))
    for x, y, c, lb in zip(ind, key, my_colors, value):
        ax.bar(x, y, width, color=c, label=lb)

    # add some
    ax.set_ylabel('Count')
    ax.set_xlabel('Words')
    ax.set_title('Trending words and their counts')
    ax.set_xticks(ind + width / 2)
    ax.legend(prop={'size':6})
    ax.get_xaxis().set_ticks([])
    plt.setp(ax.get_xticklabels(), fontsize=7, rotation=45)

    plt.savefig(trendsPng)


    N = len(key1)

    ind = np.arange(N)  # the trends locations for the groups
    width = 0.35  # the width of the bars

    fig1, ax = plt.subplots()
    my_colors = list(islice(cycle(['#0033CC', '#00FFCC', '#FF0066', '#FF9900', '#009933', '#660066', '#663300', '#003300', '#666633', '#3333CC']), None, N))
    for x, y, c, lb in zip(ind, key1, my_colors, value1):
        ax.bar(x, y, width, color=c, label=lb)

    # add some
    ax.set_ylabel('Count')
    ax.set_xlabel('user mentions')
    ax.set_title('Top user mentions and their counts')
    ax.set_xticks(ind + width / 2)
    ax.legend(prop={'size':6})
    ax.get_xaxis().set_ticks([])
    plt.setp(ax.get_xticklabels(), fontsize=7, rotation=45)
    # plt.show()
    plt.savefig(usersPng)


    N = len(key2)

    ind = np.arange(N)  # the trends locations for the groups
    width = 0.35  # the width of the bars

    fig2, ax = plt.subplots()
    my_colors = list(islice(cycle(['#0033CC', '#00FFCC', '#FF0066', '#FF9900', '#009933', '#660066', '#663300', '#003300', '#666633', '#3333CC']), None, N))
    for x, y, c, lb in zip(ind, key2, my_colors, value2):
        ax.bar(x, y, width, color=c, label=lb)

    # add some
    ax.set_ylabel('Count')
    ax.set_xlabel('hashtags')
    ax.set_title('Top hashtags and their counts')
    ax.set_xticks(ind + width / 2)
    ax.legend(prop={'size':6})
    ax.get_xaxis().set_ticks([])
    plt.setp(ax.get_xticklabels(), fontsize=7, rotation=45)
    # plt.show()
    plt.savefig(hashtagPng)

    # END of chart generation

    # Begin the pdf generation
    pp = PdfPages('report.pdf')
    pp.savefig(fig)
    pp.savefig(fig1)
    pp.savefig(fig2)
    pp.close()

    # END pdf generation and open the pdf
    print '\nAll processing done.'
    option = raw_input('Do you wish to see the charts(pdf)? [y|n]: ')
    if 'y' in option:
        os.system('open report.pdf')

    print '\nRun more ' + trendsFile + ' to see the list of trending words'
    print 'Run more ' + userFile + ' to see the list of trending words'
    print 'Run more ' + hashtagFile + ' to see the list of trending words'

    print '\nAll done. Exiting. You can see the directory to see generated PNG\'s and PDF report.'

if __name__ == "__main__":
    main()
