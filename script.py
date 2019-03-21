from collections import defaultdict
from lxml import etree
from queue import PriorityQueue
from bz2file import BZ2File
import re
import os.path
import csv
import json
import time

# User inputs
FILENAME_CSV = "categorylinkspage-join.csv"
FILENAME_ARTICLES_XML = "enwiki-20190101-pages-articles-multistream.xml"
wantedCategory = "Computer_hardware"
maxDepth = 3

# Create paths
RESOURCES_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                              "resources")
OUTPUT_PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                           "output")

if not os.path.isdir(OUTPUT_PATH):
    try:
        os.mkdir(OUTPUT_PATH)
    except OSError:
        print("Creating of directory %s failed" % OUTPUT_PATH)
    else:
        print("Successfully created the directory %s" % OUTPUT_PATH)

# Set full input file paths
INPUT_FILEPATH_CSV_DUMP = os.path.join(RESOURCES_PATH, FILENAME_CSV)
INPUT_FILEPATH_ARTICLES_XML_BZ2 = os.path.join(RESOURCES_PATH, FILENAME_ARTICLES_XML)

# Set output file paths
PATH_CATEGORYLINKS_JSON_FILE = os.path.join(RESOURCES_PATH, "categories-" +
                                            FILENAME_CSV.split(".")[
                                                0] + ".json")
PATH_ARTICLES_ID_CAT_CSV_FILE = os.path.join(RESOURCES_PATH,
                                             "articles-" + FILENAME_CSV)


def printTime(start, end):
    elapsed_time = end - start
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time - minutes * 60)
    print("Elapsed time: %sm %ss" % (minutes, seconds))


regexps_dict = {
    '\[\[+(Category:)(.*?)\]+\]|\[\[(?:[^\]|]*\|)?([^\]|]*)\]\]': r'\3',
    '\[\[+(File:)(.*?)\]+\]': "",
    '&(amp;)?#([a-zA-Z0-9]*);': ""
}

regexp_title = re.compile('[^\w\s-]')

def normalize(str):
    # return re.sub(regexp_title, "", str).replace("_"," ")
    return str.replace("_", " ")


def clean_text(text, regexps_dict):
    for k, v in regexps_dict.items():
        text = re.sub(k, v, text)
    return text.encode('ascii', 'ignore')


def save_as_csv(dict, output_path):
    with open(output_path + ".csv", "w", encoding="latin-1") as csv_file:
        csvwriter = csv.writer(csv_file, delimiter=",", lineterminator="\n",
                               quoting=csv.QUOTE_ALL)
        for k, vs in dict.items():
            if not isinstance(vs, (list,)):
                vs = [vs]
            csvwriter.writerow([k] + vs)
        print("%s saved." % output_path)


def save_as_json(dict, output_path):
    with open(output_path + ".json", "w", encoding="utf-8") as f:
        json.dump(dict, f)
        print("%s saved." % output_path)


def csvdump_extractor(inputfile_csv, outputfile_articles_csv,
                      outputfile_categories_json):
    print(
        "Extracting all categories and articles from %s\n..." % inputfile_csv)
    start = time.time()
    try:
        with open(inputfile_csv, "r", encoding="latin-1") as csv_file, open(
                outputfile_articles_csv, "w",
                encoding="utf-8") as new_csv_file:
            dialect = csv.Sniffer().sniff(csv_file.read(1024))
            dialect.escapechar = "\\"
            dialect.quoting = csv.QUOTE_MINIMAL
            csv_file.seek(0)
            reader = csv.reader(csv_file, dialect)
            category_to_subcategory = defaultdict(list)
            csvwriter = csv.writer(new_csv_file, delimiter=",", escapechar=" ",
                                   lineterminator="\n",
                                   quoting=csv.QUOTE_ALL)
            for line in reader:
                try:
                    # page_id = 0, page_namespace = 1, page_title = 2, cl_to = 15
                    cat = normalize(line[15])
                    if line[1] == "14":
                        subcat = normalize(line[2])
                        category_to_subcategory[cat].append(subcat)
                        category_to_subcategory[subcat]
                    if line[1] == "0":
                        csvwriter.writerow(
                            [line[0], line[2].replace("\\", "\\\\"), cat])
                except IOError as e:
                    print("Skipped line: %s" % line)
                    continue
            end = time.time()
            printTime(start, end)
            print("%s saved." % outputfile_articles_csv)
            save_as_json(category_to_subcategory, outputfile_categories_json)
    except FileNotFoundError as e:
        print("Inputfile not found:", inputfile_csv)
        raise e


def getcategorydepths(cat_to_subcats, wantedcategory, maxdepth):
    print("\nCollecting all subcategories for \'%s\' (Max depth: %s)" % (
        wantedcategory, maxdepth))
    cat_depth = defaultdict(int)
    if not wantedcategory in cat_to_subcats.keys():
        return cat_depth
    cat_depth[wantedcategory] = 0
    queue = PriorityQueue()
    queue.put((0, wantedcategory))
    while not queue.empty():
        (d, category) = queue.get()
        if d >= maxdepth:
#            print(
#                "Maximum depth %s reached. Next subcategory in queue: \'%s\'" % (
#                    d, category))
            break
        try:
            subcategories = cat_to_subcats[category]
            for s in subcategories:
                if s not in cat_depth.keys():
                    cat_depth[s] = d + 1
                    queue.put((d + 1, s))
        except KeyError:
            print(
                "KeyError: Skipped category \'%s\'" % category)
            cat_depth.pop(category, None)
            continue
    print("Found %s categories for starting category \'%s\', max depth %s)" % (
        len(cat_depth), wantedcategory, maxdepth))
    return cat_depth


def getsubcats(cat_to_subcats, cat_depth):
    categorylinks = {}
    for cat in cat_depth.keys():
        if cat in cat_to_subcats.keys():
            categorylinks[cat] = cat_to_subcats[cat]
    return categorylinks


def collectArticleIds(path, cat_to_depth):
    print("Collecting article ids for \'%s\' (Max depth: %s)..." % (
        wantedCategory, maxDepth))
    start = time.time()
    try:
        with open(path, "r", encoding="latin-1") as csv_file:
            dialect = csv.Sniffer().sniff(csv_file.read(1024))
            dialect.escapechar = "\\"
            #dialect.quotechar = "|"
            dialect.quoting = csv.QUOTE_MINIMAL
            csv_file.seek(0)
            reader = csv.reader(csv_file, dialect)
            category_to_articleids = defaultdict(list)
            count = 0
            for line in reader:
                try:
                    page_id = line[0]
                    cl_to = line[2]
                    if cl_to in cat_to_depth.keys():
                        category_to_articleids[cl_to].append(page_id)
                        count += 1
                except:
                    print("Skipped line: %s" % line)
                    continue
            end = time.time()
            print("Article ids collected: %s" % count)
            printTime(start, end)
            return category_to_articleids
    except FileNotFoundError as e:
        print("Inputfile not found:\n", path)
        raise e


# XML Headers
Header = "http://www.mediawiki.org/xml/export-0.10/"
Tagheader = "{" + Header + "}"
Tnamespaces = Tagheader + "namespaces"
Tpage = Tagheader + "page"
Ttitle = Tagheader + "title"
Tid = Tagheader + "id"
Trev = Tagheader + "revision"
Ttext = Tagheader + "text"


def create_subelem(parent, name, content):
    newchild = etree.SubElement(parent, name)
    newchild.text = content
    return newchild


def create_page(elem, title_path, id_path, text_path, articleids, newfile):
    id = id_path(elem)[0].text
    title = normalize(title_path(elem)[0].text)
    text = clean_text(text_path(elem)[0].text, regexps_dict)
    newpage = etree.Element("page")
    create_subelem(newpage, "title", title)
    create_subelem(newpage, "id", id)
    create_subelem(etree.SubElement(newpage, "revision"), "text", text)
    newfile.write(newpage)#, pretty_print=True)


def create_namespace(elem, newfile):
    newnamespaces = etree.Element("namespaces")
    with newfile.element("siteinfo"):
        for child in elem:
            nc = create_subelem(newnamespaces, "namespace", child.text)
            for k, v in child.attrib.items():
                nc.attrib[k] = v
        newfile.write(newnamespaces)#, pretty_print=True)


def articlecollector(path_articles_xml, outpath_articles, articleids):
    print("\nCollecting articles for \'%s\' from %s\n..." % (
        wantedCategory, path_articles_xml))
    title_path = etree.ETXPath("child::" + Ttitle)
    id_path = etree.ETXPath("child::" + Tid)
    text_path = etree.ETXPath("child::" + Trev + "/" + Ttext)
    start = time.time()
    with BZ2File(outpath_articles, "w", compresslevel=9) as file:
        context = etree.iterparse(path_articles_xml, events=("end",),
                                  tag={Tnamespaces, Tpage})
        with etree.xmlfile(file, encoding="utf-8") as newfile:
            #newfile.write_declaration(standalone=True)
            with newfile.element("mediawiki",
                                 xmlns=Header):
                for action, elem in context:
                    if elem.tag == Tpage and id_path(elem)[
                        0].text in articleids:
                        create_page(elem, title_path, id_path, text_path,
                                    articleids, newfile)
                    elif elem.tag == Tnamespaces:
                        create_namespace(elem, newfile)
                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]
    end = time.time()
    printTime(start, end)


def main():
    # Extract relevant information from .csv dump (if needed)
    if not (os.path.isfile(PATH_ARTICLES_ID_CAT_CSV_FILE) and os.path.isfile(
            PATH_CATEGORYLINKS_JSON_FILE)):
        csvdump_extractor(INPUT_FILEPATH_CSV_DUMP, PATH_ARTICLES_ID_CAT_CSV_FILE,
                          PATH_CATEGORYLINKS_JSON_FILE)

    # Load category to subcategory dict
    with open(FILEPATH_CATEGORYLINKS_JSON_FILE, "r") as f:
        cat_to_subcats_full = json.load(f)

    # For a given starting category and maximum depth get all subcategories
    # and their depth
    cat_to_depth = getcategorydepths(cat_to_subcats_full, normalize(
        wantedCategory),
                                     maxDepth)
    filepath_cat_to_depth = os.path.join(OUTPUT_PATH,
                                      wantedCategory.replace(" ", "_")
                                      + "_category_to_depth-d" + str(maxDepth))
    save_as_json(cat_to_depth, filepath_cat_to_depth)
    save_as_csv(cat_to_depth, filepath_cat_to_depth)

    cat_to_subcats = getsubcats(cat_to_subcats_full, cat_to_depth)
    filepath_cat_to_subcat = os.path.join(OUTPUT_PATH,
                                      wantedCategory.replace(" ", "_")
                                      + "_categorylinks-d" + str(maxDepth))
    save_as_json(cat_to_subcats, filepath_cat_to_subcat)
    save_as_csv(cat_to_subcats, filepath_cat_to_subcat)

    # Collect article ids collected categories
    filepath_cat_to_artids = os.path.join(OUTPUT_PATH,
                                      wantedCategory.replace(" ", "_")
                                      + "_category_to_articleids-d" + str(
                                          maxDepth))
    if not os.path.isfile(filepath_cat_to_artids + ".json"):
        print("Collecting articleids")
        cat_to_articleids = collectArticleIds(FILEPATH_ARTICLES_ID_CAT_CSV_FILE,
                                              cat_to_depth)
        save_as_json(cat_to_articleids, filepath_cat_to_artids)
        save_as_csv(cat_to_articleids, filepath_cat_to_artids)
    else:
        with open(filepath_cat_to_artids + ".json", "r") as f:
            print("Loading article ids")
            cat_to_articleids = json.load(f)
    cat_to_depth.clear()
    cat_to_subcats.clear()

    articleids = set()
    count = 0
    for cat, ids in cat_to_articleids.items():
        for id in ids:
            count += 1
            articleids.add(id)
    cat_to_articleids.clear()
    print("%s Category-articleID pairs found" % count)
    print("%s unique articleIDs" % len(articleids))

    # Extract articles from XML file
    filepath_articles_xml_bz2 = os.path.join(OUTPUT_PATH,
                                          wantedCategory.replace(" ",
                                                                 "_") +
                                          "_articles-d"
                                          + str(maxDepth) + ".xml.bz2")
    articlecollector(INPUT_FILEPATH_ARTICLES_XML_BZ2, filepath_articles_xml_bz2, articleids)


if __name__ == "__main__":
    main()