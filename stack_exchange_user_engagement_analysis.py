import pyspark
from pyspark.sql import SparkSession
import pprint
import json
from pyspark.sql.types import StructType, FloatType, LongType, StringType, StructField, TimestampType
from pyspark.sql import Window
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import lead, udf, struct, col
from internetarchive import download
from internetarchive import get_item
import os
import pandas as pd
import pathlib
import py7zr
from xml.etree.ElementTree import iterparse

from google.cloud import storage
from google.cloud.storage import Blob


spark = SparkSession.builder.getOrCreate()
sc = pyspark.SparkContext()

ia_identifier = 'stackexchange'
cwd = os.getcwd()
zipped = "gaming.stackexchange.com.7z"
# 'mythology.stackexchange.com.7z'            # mythology for testing
# 'gaming.stackexchange.com.7z'  # For project
# 'unix.stackexchange.com.7z'  TOO BIG
subdir = zipped.rsplit('.', 1)[0]
extract_path = f'{cwd}/{ia_identifier}/{subdir}'

bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
xml_directory = 'gs://{}'.format(bucket)
input_directory = 'gs://{}/hadoop/tmp/bigquerry/pyspark_input'.format(bucket)
output_directory = 'gs://{}/pyspark_demo_output'.format(bucket)

conf={
    'mapred.bq.project.id':project,
    'mapred.bq.gcs.bucket':bucket,
    'mapred.bq.temp.gcs.path':input_directory,
    'mapred.bq.input.project.id': 'cs-512-jem-374507',
    'mapred.bq.input.dataset.id': 'aircraft_data',
    'mapred.bq.input.table.id': 'cs512_data_20230223_215917',
}

# (JEM) Cleanup temp path before execution
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)


item = get_item(ia_identifier)
print(f"FULL ARCHIVE SIZE of {ia_identifier}: {round(item.item_size / 1024 / 1024 / 1024, 2)} GB")

pd.set_option("display.max_colwidth", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.expand_frame_repr", False)


def getPathSize(_path):
    size = 0
    for f in os.listdir(_path):
        path = os.path.join(_path, f)
        if os.path.isfile(path):
            size += os.path.getsize(path)
    size = size / 1024 / 1024 / 1024
    return size


def list_files():
    """
    Lists all files in item object and prints the compressed folder size and converts to MB
    :return:
    """
    for x in item.item_metadata['files']:
        if 'size' in x.keys():
            print(f"{x['name']}, {round(int(x['size']) / 1024 / 1024, 2)} MB")


def download_items(zipfile, cwd):

    download(ia_identifier, dry_run=False, verbose=True, files=zipfile)
    subdir = zipfile.rsplit('.', 1)[0]
    full_path = f'{cwd}/{ia_identifier}/{subdir}'

    #GCP
    storage_client = storage.Client("[cs-512-jem-374507]")
    bucket = storage_client.get_bucket("cs512final_airmax")


    # Clean up directory if path exists
    if os.path.exists(full_path):
        for f in os.listdir(full_path):
            os.remove(os.path.join(full_path, f))
        os.rmdir(full_path)
    # Decompress 7z file
    with py7zr.SevenZipFile(f'{cwd}/{ia_identifier}/{zipfile}', mode='r') as z:
        z.extractall(full_path)
    # Get decompressed directory size in GB
    dir_size = getPathSize(full_path)
    print(f"DIRECTORY SIZE DECOMPRESSED IS {dir_size} GB")
    dirpath = pathlib.Path(full_path)
    dirpath.glob("*")
    dir_list = list(dirpath.glob("*"))
    for f in dir_list:
        local_file = f"{f}"
        filename = os.path.basename(local_file)
        blob = Blob(filename, bucket)
        blob.upload_from_filename(local_file)


def get_comments():
    """
    :param path: The local file path to get to the Comments.xml file and extract into a DataFrame
    :return: Pandas DataFrame
    """
    data_columns = ['Id', 'PostId', 'Score', 'Text', 'CreationDate', 'UserId', 'ContentLicense']
    rootTag = "comments"
    rowTag = "row"
    fileName = "Comments.xml"

    # customSchema = StructType([
    #     StructField("Id", LongType(), True),
    #     StructField("PostId", LongType(), True),
    #     StructField("Score", LongType(), True),
    #     StructField("Text", StringType(), True),
    #     StructField("CreationDate", TimestampType(), True),
    #     StructField("UserId", LongType(), True),
    #     StructField("ContentLicense", StringType(), True)])

    df = spark.read.format('xml') \
        .options(rootTag=rootTag) \
        .options(rowTag=rowTag)\
        .load(f'{xml_directory}/{fileName}')
    df.show(5)
    df.dtypes
    df.describe()
    return df


def get_posts(path):
    """
    :param path: The local file path to get to the Posts.xml file and extract into a DataFrame
    :return: Pandas DataFrame
    """
    post_columns = ['Id', 'PostTypeId', 'AcceptedAnswerId', 'CreationDate', 'Score', 'ViewCount', 'Body', 'OwnerUserId',
                    'LastEditorUserId', 'LastEditDate', 'LastActivityDate', 'Title', 'Tags', 'AnswerCount',
                    'CommentCount',
                    'ContentLicense']
    df = pd.DataFrame(columns=post_columns)
    i = 0
    for _, elem in iterparse(f"{path}/Posts.xml", events=("end",)):
        tmp_df = pd.DataFrame(elem.attrib, index=[0])
        df = pd.concat([df, tmp_df], ignore_index=True)
        i += 1
        if i % 1000 == 0:
            print(i)
    post_df = df.reset_index()
    post_df.drop(columns=['index'], inplace=True)
    return post_df


def get_posthistory(path):
    """
    :param path: The local file path to get to the PostHistory.xml file and extract into a DataFrame
    :return: Pandas DataFrame
    """
    posthistory_columns = ['Id', 'PostHistoryTypeId', 'PostId', 'RevisionGUID', 'CreationDate', 'UserId', 'Text',
                           'ContentLicense']
    df = pd.DataFrame(columns=posthistory_columns)
    i = 0
    for _, elem in iterparse(f"{path}/PostHistory.xml", events=("end",)):
        tmp_df = pd.DataFrame(elem.attrib, index=[0])
        df = pd.concat([df, tmp_df], ignore_index=True)
        i += 1
        if i % 1000 == 0:
            print(i)
    posthistory_df = df.reset_index()
    posthistory_df.drop(columns=['index'], inplace=True)
    return posthistory_df


def getPostType(row):
    """
    Lookup of Post Type to recursively update and add column to Posts
    :param row: row dictionary
    :return: Post Type string
    """
    if row['PostTypeId'] == 1:
        return 'Question'
    if row['PostTypeId'] == 2:
        return 'Answer'
    if row['PostTypeId'] == 3:
        return 'Orphaned tag wiki'
    if row['PostTypeId'] == 4:
        return 'Tag wiki excerpt'
    if row['PostTypeId'] == 5:
        return 'Tag wiki'
    if row['PostTypeId'] == 6:
        return 'Moderator nomination'
    if row['PostTypeId'] == 7:
        return 'Wiki placeholder'
    if row['PostTypeId'] == 8:
        return 'Privilege wiki'


def getBadgeClass(row):
    """
    Lookup of Class Type to recursively update and add column to Badges
    :param row: row dictionary
    :return: Class string
    """
    if row['Class'] == 1:
        return 'Gold'
    if row['Class'] == 2:
        return 'Silver'
    if row['Class'] == 3:
        return 'Bronze'


def getLinkType(row):
    """
    Lookup of Link Type to recursively update and add column to Post Links
    :param row: row dictionary
    :return: Link Type string
    """
    if row['LinkTypeId'] == 1:
        return 'Linked'
    if row['LinkTypeId'] == 3:
        return 'Duplicate'


def getVoteTypes(row):
    if row['VoteTypeId'] == 1:
        return "AcceptedByOriginator"
    if row['VoteTypeId'] == 2:
        return "UpMod"
    if row['VoteTypeId'] == 3:
        return "DownMod"
    if row['VoteTypeId'] == 4:
        return "Offensive"
    if row['VoteTypeId'] == 5:
        return "Favorite"
    if row['VoteTypeId'] == 6:
        return "Close"
    if row['VoteTypeId'] == 7:
        return "Reopen"
    if row['VoteTypeId'] == 8:
        return "BountyStart"
    if row['VoteTypeId'] == 9:
        return "BountyClose"
    if row['VoteTypeId'] == 10:
        return "Deletion"
    if row['VoteTypeId'] == 11:
        return "Undeletion"
    if row['VoteTypeId'] == 12:
        return "Spam"
    if row['VoteTypeId'] == 15:
        return "ModeratorReview"
    if row['VoteTypeId'] == 16:
        return "ApproveEditSuggestion"


def getPostHistoryType(row):
    if row['PostHistoryTypeId'] == 1: return "Initial Title"
    if row['PostHistoryTypeId'] == 2: return "Initial Body"
    if row['PostHistoryTypeId'] == 3: return "Initial Tags"
    if row['PostHistoryTypeId'] == 4: return "Edit Title"
    if row['PostHistoryTypeId'] == 5: return "Edit Body"
    if row['PostHistoryTypeId'] == 6: return "Edit Tags"
    if row['PostHistoryTypeId'] == 7: return "Rollback Title"
    if row['PostHistoryTypeId'] == 8: return "Rollback Body"
    if row['PostHistoryTypeId'] == 9: return "Rollback Tags"
    if row['PostHistoryTypeId'] == 10: return "Post Closed"
    if row['PostHistoryTypeId'] == 11: return "Post Reopened"
    if row['PostHistoryTypeId'] == 12: return "Post Deleted"
    if row['PostHistoryTypeId'] == 13: return "Post Undeleted"
    if row['PostHistoryTypeId'] == 14: return "Post Locked"
    if row['PostHistoryTypeId'] == 15: return "Post Unlocked"
    if row['PostHistoryTypeId'] == 16: return "Community Owned"
    if row['PostHistoryTypeId'] == 17: return "Post Migrated "
    if row['PostHistoryTypeId'] == 18: return "Question Merged - question merged with deleted question"
    if row['PostHistoryTypeId'] == 19: return "Question Protected - question was protected by a moderator."
    if row['PostHistoryTypeId'] == 20: return "Question Unprotected - question was unprotected by a moderator."
    if row['PostHistoryTypeId'] == 21: return "Post Disassociated - OwnerUserId removed from post by admin"
    if row['PostHistoryTypeId'] == 22: return "Question Unmerged - answers/votes restored to previously merged question"
    if row['PostHistoryTypeId'] == 24: return "Suggested Edit Applied"
    if row['PostHistoryTypeId'] == 25: return "Post Tweeted"
    if row['PostHistoryTypeId'] == 31: return "Comment discussion moved to chat"
    if row['PostHistoryTypeId'] == 33: return "Post notice added"
    if row['PostHistoryTypeId'] == 34: return "Post notice removed"
    if row['PostHistoryTypeId'] == 35: return "Post migrated away"
    if row['PostHistoryTypeId'] == 36: return "Post migrated here"
    if row['PostHistoryTypeId'] == 37: return "Post merge source"
    if row['PostHistoryTypeId'] == 38: return "Post merge destination"
    if row['PostHistoryTypeId'] == 50: return "Bumped by Community User"
    if row['PostHistoryTypeId'] == 52: return "Question became hot network question (main) / Hot Meta question (meta)"
    if row['PostHistoryTypeId'] == 53: return "Question removed from hot network/meta questions by a moderator"


def get_data(path):
    """
    :param path: The local file path to get to the xml files and extracts into DataFrames
    :return: N/A
    """

    # 1
    post_df = get_posts(path)
    post_df['PostTypeId'] = post_df['PostTypeId'].astype("Int64")
    post_df['AcceptedAnswerId'] = post_df['AcceptedAnswerId'].astype("Int64")
    post_df['ParentId'] = post_df['ParentId'].astype("Int64")
    post_df['OwnerUserId'] = post_df['OwnerUserId'].astype("Int64")
    post_df['LastEditorUserId'] = post_df['LastEditorUserId'].astype("Int64")
    post_df['PostTypeId'].fillna(0, inplace=True)
    print(f"post_df\n{post_df.dtypes}")
    print(f"post_df unique\n{post_df['PostTypeId'].unique()}")
    post_df['PostType'] = post_df.apply(lambda row: getPostType(row), axis=1)
    post_cols = post_df.columns.tolist()
    print(f"post_df {post_cols}")
    print(f"post_df\n{post_df.dtypes}")
    print(f"{post_df.head(5)}")
    post_df.to_csv(f"{extract_path}/posts.csv", sep="\036", index=False)
    post_df.to_json(f"{extract_path}/posts.json", orient='records', lines=True)

    # 2
    posthistory_df = get_posthistory(path)
    posthistory_df['PostHistoryTypeId'] = posthistory_df['PostHistoryTypeId'].astype("Int64")
    posthistory_df['PostHistoryTypeId'].fillna(0, inplace=True)
    posthistory_df['UserId'] = posthistory_df['UserId'].astype("Int64")
    print(f"posthistory_df {posthistory_df.dtypes}")
    posthistory_df['PostHistoryType'] = posthistory_df.apply(lambda row: getPostHistoryType(row), axis=1)
    print(f"posthistory_df {posthistory_df.columns.tolist()}")
    print(f"{posthistory_df.head(5)}")
    posthistory_df.to_csv(f"{extract_path}/posthistory.csv", sep="\036", index=False)
    posthistory_df.to_json(f"{extract_path}/posthistory.json", orient='records', lines=True)

    # 3
    users_df = get_dataframe(path, 'Users.xml')
    print(f"users_df {users_df.dtypes}")
    print(f"users_df {users_df.columns.tolist()}")
    print(f"{users_df.head(5)}")
    users_df.to_csv(f"{extract_path}/users.csv", sep="\036", index=False)
    users_df.to_json(f"{extract_path}/users.json", orient='records', lines=True)

    # 4
    votes_df = get_dataframe(path, 'Votes.xml')
    votes_df['PostId'] = votes_df['PostId'].astype("Int64")
    votes_df['VoteTypeId'] = votes_df['VoteTypeId'].astype("Int64")
    votes_df['VoteTypeId'].fillna(0, inplace=True)
    votes_df['UserId'] = votes_df['UserId'].astype("Int64")
    print(f"votes_df {votes_df.dtypes}")
    votes_df['VoteType'] = votes_df.apply(lambda row: getVoteTypes(row), axis=1)
    print(f"votes_df {votes_df.columns.tolist()}")
    print(f"{votes_df.head(5)}")
    votes_df.to_csv(f"{extract_path}/votes.csv", sep="\036", index=False)
    votes_df.to_json(f"{extract_path}/votes.json", orient='records', lines=True)

    # 5
    tags_df = get_dataframe(path, 'Tags.xml')
    tags_df['ExcerptPostId'] = tags_df['ExcerptPostId'].astype("Int64")
    tags_df['WikiPostId'] = tags_df['WikiPostId'].astype("Int64")
    print(f"tags_df {tags_df.dtypes}")
    print(f"tags_df {tags_df.columns.tolist()}")
    print(f"{tags_df.head(5)}")
    tags_df.to_csv(f"{extract_path}/tags.csv", sep="\036", index=False)
    tags_df.to_json(f"{extract_path}/tags.json", orient='records', lines=True)

    # 6
    postLinks_df = get_dataframe(path, 'PostLinks.xml')
    postLinks_df['PostId'] = postLinks_df['PostId'].astype("Int64")
    postLinks_df['RelatedPostId'] = postLinks_df['RelatedPostId'].astype("Int64")
    postLinks_df['LinkTypeId'] = postLinks_df['LinkTypeId'].astype("Int64")
    postLinks_df['LinkType'] = postLinks_df.apply(lambda row: getLinkType(row), axis=1)
    print(f"postLinks_df {postLinks_df.dtypes}")
    postLinks_cols = postLinks_df.columns.tolist()
    print(f"postLinks_df {postLinks_cols}")
    print(f"{postLinks_df.head(5)}")
    postLinks_df.to_csv(f"{extract_path}/postLinks.csv", sep="\036", index=False)
    postLinks_df.to_json(f"{extract_path}/postLinks.json", orient='records', lines=True)

    # 7
    badges_df = get_dataframe(path, 'Badges.xml')
    badges_df['UserId'] = badges_df['UserId'].astype("Int64")
    badges_df['Class'] = badges_df['Class'].astype("Int64")
    print(f"badges_df {badges_df.dtypes}")
    badges_df['ClassType'] = badges_df.apply(lambda row: getBadgeClass(row), axis=1)
    badges_cols = badges_df.columns.tolist()
    print(f"badges_df {badges_cols}")
    print(f"{badges_df.head(5)}")
    badges_df.to_csv(f"{extract_path}/badges.csv", sep="\036", index=False)
    badges_df.to_json(f"{extract_path}/badges.json", orient='records', lines=True)

    # 8
    comments_df = get_comments(path)
    comments_df['PostId'] = comments_df['PostId'].astype("Int64")
    comments_df['UserId'] = comments_df['UserId'].astype("Int64")
    print(f"comments_df {comments_df.dtypes}")
    print(f"comments_df {comments_df.columns.tolist()}")
    print(f"{comments_df.head(5)}")
    comments_df.to_csv(f"{extract_path}/comments.csv", sep="\036", index=False)
    comments_df.to_json(f"{extract_path}/comments.json", orient='records', lines=True)


def get_dataframe(path, filename):
    """
    Gets an XML file and converts to Pandas DataFrame
    :param path: Path to local XML file
    :param filename: name of XML file
    :return: Pandas DataFrame
    """
    df = pd.read_xml(f'{path}/{filename}')
    print(df.columns.tolist())
    return df


def all_steps():
    """
    Download StackExchange compressed file and decompress and convert XML data to DataFrames
    :return:
    """
    download_items(zipped, cwd)
    #get_data(extract_path)


if __name__ == "__main__":
    #all_steps()
    get_comments()
