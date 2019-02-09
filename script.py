from collections import defaultdict
from queue import PriorityQueue
import os.path
import csv
import json
import sys
import time

inputFile = './resources/categorylinks_pages-join-sample.csv'
out_categorylinks = './resources/categorylinks.json'
out_depth = './resources/subcategories_by_depth.json'
out_subcategories = './resources/depth_by_subcategories.json'
out_articleids = './resources/articleids_by_category.json'
category = 'Computer_hardware'
depth_subcats = sys.maxsize
depth_articles = sys.maxsize


def printTime(start, end):
    elapsed_time = end - start
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time - minutes * 60)
    print('Time needed: %sm %ss\n' % (minutes, seconds))


def invert_dict(d):
    newdict = defaultdict(list)
    for k, v in d.items():
        newdict[v].append(k)
    return newdict


def buildCategorylinks(path):
    print('Building categorylinks...')
    start = time.time()
    try:
        with open(path, 'r', encoding='latin-1') as csv_file:
            dialect = csv.Sniffer().sniff(csv_file.read(1024))
            dialect.escapechar = '\\'
            dialect.quoting = csv.QUOTE_MINIMAL
            csv_file.seek(0)
            reader = csv.reader(csv_file, dialect)
            category_to_subcategory = defaultdict(list)
            for line in reader:
                page_namespace = int(line[1])
                page_title = line[2]
                cl_to = line[15]
                if (len(line) != 21):
                    print('Skipped line: %s' % line)
                if page_namespace == 14:
                    category_to_subcategory[cl_to].append(page_title)
                    if page_title not in category_to_subcategory:
                        category_to_subcategory[page_title] = []
            end = time.time()
            print('Categorylinks built')
            printTime(start, end)
            save(out_categorylinks, category_to_subcategory)
            return category_to_subcategory
    except FileNotFoundError as e:
        print("Inputfile not found:", path)
        raise e


def save(path, dict):
    with open(path, 'w', encoding='latin-1') as f:
        print('Saving result in %s' % path)
        json.dump(dict, f)
        print('Saved.\n')


def getAllSubcategories(wantedCategory, maxdepth):
    print('Finding all subcategories for %s' % (wantedCategory))
    cat_depth = defaultdict(int)
    cat_depth[wantedCategory] = 0
    queue = PriorityQueue()
    queue.put((0, wantedCategory))
    while not queue.empty():
        (d, category) = queue.get()
        if not d < maxdepth:
            print('Maximum depth %s reached. Next subcategory in queue: %s' % (
                d, category))
            break
        else:
            try:
                subcategories = category_to_subcategory[category]
                for s in subcategories:
                    if s not in cat_depth.keys():
                        cat_depth[s] = d + 1
                        queue.put((d + 1, s))
            except KeyError:
                print(
                    'KeyError: Skipping category %s: does not exist in %s' % (
                        category, out_categorylinks))
                cat_depth.pop(category, None)
                continue
    print('Found %s categories\n' % len(cat_depth))
    return cat_depth


def getAllArticleIds(path, categories_by_depth, maxdepth):
    print("Collecting article ids...")
    start = time.time()
    try:
        with open(path, 'r', encoding="latin-1") as csv_file:
            dialect = csv.Sniffer().sniff(csv_file.read(1024))
            dialect.escapechar = '\\'
            dialect.quoting = csv.QUOTE_MINIMAL
            csv_file.seek(0)
            reader = csv.reader(csv_file, dialect)
            category_to_articleids = defaultdict(list)
            for line in reader:
                page_namespace = int(line[1])
                page_id = line[0]
                cl_to = line[15]
                if (len(line) != 21):
                    print('Skipped line: %s' % line)
                if page_namespace == 0 and cl_to in categories_by_depth.keys()\
                        and categories_by_depth[cl_to] < maxdepth:
                    category_to_articleids[cl_to].append(page_id)
            end = time.time()
            print('Article ids collected: %s' % len(category_to_articleids))
            printTime(start, end)
            return category_to_articleids
    except FileNotFoundError as e:
        print("Inputfile not found:", path)
        raise e


try:
    if not os.path.isfile(out_categorylinks):
        buildCategorylinks(inputFile)
    with open(out_categorylinks, 'r') as f:
        category_to_subcategory = json.load(f)
        print('%s loaded\n' % out_categorylinks)
        if not os.path.isfile(out_depth):
            subcats = getAllSubcategories(category, depth_subcats)
            save(out_depth, subcats)
        with open(out_depth, 'r') as f2:
            subcats = json.load(f2)
            save(out_subcategories, invert_dict(subcats))
            if not os.path.isfile(out_articleids):
                articleids = getAllArticleIds(inputFile, subcats, depth_articles)
                save(out_articleids, articleids)
except:
    raise
