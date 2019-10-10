# pylint: skip-file
# flake8: noqa
import sys
import boto3
from collections import defaultdict, OrderedDict
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import *
from urlparse import urlparse
from pyspark.sql.functions import col, broadcast
import tarfile
from io import BytesIO
from datetime import datetime
from multiprocessing import Pool
from contextlib import closing

# Read arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3target', 's3source'])

# Initialise the glue job
sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define massive list of fields in the schema
fields = [
    StructField("accept_language", StringType(), True),
    StructField("browser", LongType(), True),
    StructField("browser_height", LongType(), True),
    StructField("browser_width", LongType(), True),
    StructField("c_color", StringType(), True),
    StructField("campaign", StringType(), True),
    StructField("carrier", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("click_action", StringType(), True),
    StructField("click_action_type", ShortType(), True),
    StructField("click_context", StringType(), True),
    StructField("click_context_type", ShortType(), True),
    StructField("click_sourceid", LongType(), True),
    StructField("click_tag", StringType(), True),
    StructField("code_ver", StringType(), True),
    StructField("color", LongType(), True),
    StructField("connection_type", ShortType(), True),
    StructField("cookies", StringType(), True),
    StructField("country", LongType(), True),
    StructField("ct_connect_type", StringType(), True),
    StructField("curr_factor", ShortType(), True),
    StructField("curr_rate", StringType(), True), # This was DecimalType() originally but is not compatible with Athena. See https://github.com/prestodb/presto/issues/7232
    StructField("currency", StringType(), True),
    StructField("cust_hit_time_gmt", LongType(), True),
    StructField("cust_visid", StringType(), True),
    StructField("daily_visitor", ShortType(), True),
    StructField("date_time", TimestampType(), True),
    StructField("domain", StringType(), True),
    StructField("duplicate_events", StringType(), True),
    StructField("duplicate_purchase", ShortType(), True),
    StructField("duplicated_from", StringType(), True),
    StructField("ef_id", StringType(), True),
    StructField("evar1", StringType(), True),
    StructField("evar2", StringType(), True),
    StructField("evar3", StringType(), True),
    StructField("evar4", StringType(), True),
    StructField("evar5", StringType(), True),
    StructField("evar6", StringType(), True),
    StructField("evar7", StringType(), True),
    StructField("evar8", StringType(), True),
    StructField("evar9", StringType(), True),
    StructField("evar10", StringType(), True),
    StructField("evar11", StringType(), True),
    StructField("evar12", StringType(), True),
    StructField("evar13", StringType(), True),
    StructField("evar14", StringType(), True),
    StructField("evar15", StringType(), True),
    StructField("evar16", StringType(), True),
    StructField("evar17", StringType(), True),
    StructField("evar18", StringType(), True),
    StructField("evar19", StringType(), True),
    StructField("evar20", StringType(), True),
    StructField("evar21", StringType(), True),
    StructField("evar22", StringType(), True),
    StructField("evar23", StringType(), True),
    StructField("evar24", StringType(), True),
    StructField("evar25", StringType(), True),
    StructField("evar26", StringType(), True),
    StructField("evar27", StringType(), True),
    StructField("evar28", StringType(), True),
    StructField("evar29", StringType(), True),
    StructField("evar30", StringType(), True),
    StructField("evar31", StringType(), True),
    StructField("evar32", StringType(), True),
    StructField("evar33", StringType(), True),
    StructField("evar34", StringType(), True),
    StructField("evar35", StringType(), True),
    StructField("evar36", StringType(), True),
    StructField("evar37", StringType(), True),
    StructField("evar38", StringType(), True),
    StructField("evar39", StringType(), True),
    StructField("evar40", StringType(), True),
    StructField("evar41", StringType(), True),
    StructField("evar42", StringType(), True),
    StructField("evar43", StringType(), True),
    StructField("evar44", StringType(), True),
    StructField("evar45", StringType(), True),
    StructField("evar46", StringType(), True),
    StructField("evar47", StringType(), True),
    StructField("evar48", StringType(), True),
    StructField("evar49", StringType(), True),
    StructField("evar50", StringType(), True),
    StructField("evar51", StringType(), True),
    StructField("evar52", StringType(), True),
    StructField("evar53", StringType(), True),
    StructField("evar54", StringType(), True),
    StructField("evar55", StringType(), True),
    StructField("evar56", StringType(), True),
    StructField("evar57", StringType(), True),
    StructField("evar58", StringType(), True),
    StructField("evar59", StringType(), True),
    StructField("evar60", StringType(), True),
    StructField("evar61", StringType(), True),
    StructField("evar62", StringType(), True),
    StructField("evar63", StringType(), True),
    StructField("evar64", StringType(), True),
    StructField("evar65", StringType(), True),
    StructField("evar66", StringType(), True),
    StructField("evar67", StringType(), True),
    StructField("evar68", StringType(), True),
    StructField("evar69", StringType(), True),
    StructField("evar70", StringType(), True),
    StructField("evar71", StringType(), True),
    StructField("evar72", StringType(), True),
    StructField("evar73", StringType(), True),
    StructField("evar74", StringType(), True),
    StructField("evar75", StringType(), True),
    StructField("evar76", StringType(), True),
    StructField("evar77", StringType(), True),
    StructField("evar78", StringType(), True),
    StructField("evar79", StringType(), True),
    StructField("evar80", StringType(), True),
    StructField("evar81", StringType(), True),
    StructField("evar82", StringType(), True),
    StructField("evar83", StringType(), True),
    StructField("evar84", StringType(), True),
    StructField("evar85", StringType(), True),
    StructField("evar86", StringType(), True),
    StructField("evar87", StringType(), True),
    StructField("evar88", StringType(), True),
    StructField("evar89", StringType(), True),
    StructField("evar90", StringType(), True),
    StructField("evar91", StringType(), True),
    StructField("evar92", StringType(), True),
    StructField("evar93", StringType(), True),
    StructField("evar94", StringType(), True),
    StructField("evar95", StringType(), True),
    StructField("evar96", StringType(), True),
    StructField("evar97", StringType(), True),
    StructField("evar98", StringType(), True),
    StructField("evar99", StringType(), True),
    StructField("evar100", StringType(), True),
    StructField("event_list", StringType(), True),
    StructField("exclude_hit", ShortType(), True),
    StructField("first_hit_page_url", StringType(), True),
    StructField("first_hit_pagename", StringType(), True),
    StructField("first_hit_referrer", StringType(), True),
    StructField("first_hit_time_gmt", LongType(), True),
    StructField("geo_city", StringType(), True),
    StructField("geo_country", StringType(), True),
    StructField("geo_dma", LongType(), True),
    StructField("geo_region", StringType(), True),
    StructField("geo_zip", StringType(), True),
    StructField("hier1", StringType(), True),
    StructField("hier2", StringType(), True),
    StructField("hier3", StringType(), True),
    StructField("hier4", StringType(), True),
    StructField("hier5", StringType(), True),
    StructField("hit_source", ShortType(), True),
    StructField("hit_time_gmt", LongType(), True),
    StructField("hitid_high", StringType(), True),
    StructField("hitid_low", StringType(), True),
    StructField("homepage", StringType(), True),
    StructField("hourly_visitor", ShortType(), True),
    StructField("ip", StringType(), True),
    StructField("ip2", StringType(), True),
    StructField("j_jscript", StringType(), True),
    StructField("java_enabled", StringType(), True),
    StructField("javascript", ShortType(), True),
    StructField("language", LongType(), True),
    StructField("last_hit_time_gmt", LongType(), True),
    StructField("last_purchase_num", LongType(), True),
    StructField("last_purchase_time_gmt", LongType(), True),
    StructField("mc_audiences", StringType(), True),
    StructField("mcvisid", StringType(), True),
    StructField("mobile_id", LongType(), True),
    StructField("mobileaction", StringType(), True),
    StructField("mobileappid", StringType(), True),
    StructField("mobilecampaigncontent", StringType(), True),
    StructField("mobilecampaignmedium", StringType(), True),
    StructField("mobilecampaignname", StringType(), True),
    StructField("mobilecampaignsource", StringType(), True),
    StructField("mobilecampaignterm", StringType(), True),
    StructField("mobiledayofweek", StringType(), True),
    StructField("mobiledayssincefirstuse", StringType(), True),
    StructField("mobiledayssincelastuse", StringType(), True),
    StructField("mobiledevice", StringType(), True),
    StructField("mobilehourofday", StringType(), True),
    StructField("mobileinstalldate", StringType(), True),
    StructField("mobilelaunchnumber", StringType(), True),
    StructField("mobileltv", StringType(), True),
    StructField("mobilemessageid", StringType(), True),
    StructField("mobilemessageonline", StringType(), True),
    StructField("mobileosversion", StringType(), True),
    StructField("mobilepushoptin", BooleanType(), True),
    StructField("mobilepushpayloadid", StringType(), True),
    StructField("mobileresolution", StringType(), True),
    StructField("monthly_visitor", ShortType(), True),
    StructField("mvvar1", StringType(), True),
    StructField("mvvar2", StringType(), True),
    StructField("mvvar3", StringType(), True),
    StructField("namespace", StringType(), True),
    StructField("new_visit", ShortType(), True),
    StructField("os", LongType(), True),
    StructField("p_plugins", StringType(), True),
    StructField("page_event", ShortType(), True),
    StructField("page_event_var1", StringType(), True),
    StructField("page_event_var2", StringType(), True),
    StructField("page_event_var3", StringType(), True),
    StructField("page_type", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("pagename", StringType(), True),
    StructField("paid_search", ShortType(), True),
    StructField("partner_plugins", StringType(), True),
    StructField("persistent_cookie", StringType(), True),
    StructField("plugins", StringType(), True),
    StructField("pointofinterest", StringType(), True),
    StructField("pointofinterestdistance", StringType(), True),
    StructField("post_browser_height", LongType(), True),
    StructField("post_browser_width", LongType(), True),
    StructField("post_campaign", StringType(), True),
    StructField("post_channel", StringType(), True),
    StructField("post_cookies", StringType(), True),
    StructField("post_currency", StringType(), True),
    StructField("post_cust_hit_time_gmt", LongType(), True),
    StructField("post_cust_visid", ShortType(), True),
    StructField("post_ef_id", StringType(), True),
    StructField("post_evar1", StringType(), True),
    StructField("post_evar2", StringType(), True),
    StructField("post_evar3", StringType(), True),
    StructField("post_evar4", StringType(), True),
    StructField("post_evar5", StringType(), True),
    StructField("post_evar6", StringType(), True),
    StructField("post_evar7", StringType(), True),
    StructField("post_evar8", StringType(), True),
    StructField("post_evar9", StringType(), True),
    StructField("post_evar10", StringType(), True),
    StructField("post_evar11", StringType(), True),
    StructField("post_evar12", StringType(), True),
    StructField("post_evar13", StringType(), True),
    StructField("post_evar14", StringType(), True),
    StructField("post_evar15", StringType(), True),
    StructField("post_evar16", StringType(), True),
    StructField("post_evar17", StringType(), True),
    StructField("post_evar18", StringType(), True),
    StructField("post_evar19", StringType(), True),
    StructField("post_evar20", StringType(), True),
    StructField("post_evar21", StringType(), True),
    StructField("post_evar22", StringType(), True),
    StructField("post_evar23", StringType(), True),
    StructField("post_evar24", StringType(), True),
    StructField("post_evar25", StringType(), True),
    StructField("post_evar26", StringType(), True),
    StructField("post_evar27", StringType(), True),
    StructField("post_evar28", StringType(), True),
    StructField("post_evar29", StringType(), True),
    StructField("post_evar30", StringType(), True),
    StructField("post_evar31", StringType(), True),
    StructField("post_evar32", StringType(), True),
    StructField("post_evar33", StringType(), True),
    StructField("post_evar34", StringType(), True),
    StructField("post_evar35", StringType(), True),
    StructField("post_evar36", StringType(), True),
    StructField("post_evar37", StringType(), True),
    StructField("post_evar38", StringType(), True),
    StructField("post_evar39", StringType(), True),
    StructField("post_evar40", StringType(), True),
    StructField("post_evar41", StringType(), True),
    StructField("post_evar42", StringType(), True),
    StructField("post_evar43", StringType(), True),
    StructField("post_evar44", StringType(), True),
    StructField("post_evar45", StringType(), True),
    StructField("post_evar46", StringType(), True),
    StructField("post_evar47", StringType(), True),
    StructField("post_evar48", StringType(), True),
    StructField("post_evar49", StringType(), True),
    StructField("post_evar50", StringType(), True),
    StructField("post_evar51", StringType(), True),
    StructField("post_evar52", StringType(), True),
    StructField("post_evar53", StringType(), True),
    StructField("post_evar54", StringType(), True),
    StructField("post_evar55", StringType(), True),
    StructField("post_evar56", StringType(), True),
    StructField("post_evar57", StringType(), True),
    StructField("post_evar58", StringType(), True),
    StructField("post_evar59", StringType(), True),
    StructField("post_evar60", StringType(), True),
    StructField("post_evar61", StringType(), True),
    StructField("post_evar62", StringType(), True),
    StructField("post_evar63", StringType(), True),
    StructField("post_evar64", StringType(), True),
    StructField("post_evar65", StringType(), True),
    StructField("post_evar66", StringType(), True),
    StructField("post_evar67", StringType(), True),
    StructField("post_evar68", StringType(), True),
    StructField("post_evar69", StringType(), True),
    StructField("post_evar70", StringType(), True),
    StructField("post_evar71", StringType(), True),
    StructField("post_evar72", StringType(), True),
    StructField("post_evar73", StringType(), True),
    StructField("post_evar74", StringType(), True),
    StructField("post_evar75", StringType(), True),
    StructField("post_evar76", StringType(), True),
    StructField("post_evar77", StringType(), True),
    StructField("post_evar78", StringType(), True),
    StructField("post_evar79", StringType(), True),
    StructField("post_evar80", StringType(), True),
    StructField("post_evar81", StringType(), True),
    StructField("post_evar82", StringType(), True),
    StructField("post_evar83", StringType(), True),
    StructField("post_evar84", StringType(), True),
    StructField("post_evar85", StringType(), True),
    StructField("post_evar86", StringType(), True),
    StructField("post_evar87", StringType(), True),
    StructField("post_evar88", StringType(), True),
    StructField("post_evar89", StringType(), True),
    StructField("post_evar90", StringType(), True),
    StructField("post_evar91", StringType(), True),
    StructField("post_evar92", StringType(), True),
    StructField("post_evar93", StringType(), True),
    StructField("post_evar94", StringType(), True),
    StructField("post_evar95", StringType(), True),
    StructField("post_evar96", StringType(), True),
    StructField("post_evar97", StringType(), True),
    StructField("post_evar98", StringType(), True),
    StructField("post_evar99", StringType(), True),
    StructField("post_evar100", StringType(), True),
    StructField("post_event_list", StringType(), True),
    StructField("post_hier1", StringType(), True),
    StructField("post_hier2", StringType(), True),
    StructField("post_hier3", StringType(), True),
    StructField("post_hier4", StringType(), True),
    StructField("post_hier5", StringType(), True),
    StructField("post_java_enabled", StringType(), True),
    StructField("post_keywords", StringType(), True),
    StructField("post_mc_audiences", StringType(), True),
    StructField("post_mobileaction", StringType(), True),
    StructField("post_mobileappid", StringType(), True),
    StructField("post_mobilecampaigncontent", StringType(), True),
    StructField("post_mobilecampaignmedium", StringType(), True),
    StructField("post_mobilecampaignname", StringType(), True),
    StructField("post_mobilecampaignsource", StringType(), True),
    StructField("post_mobilecampaignterm", StringType(), True),
    StructField("post_mobiledayofweek", StringType(), True),
    StructField("post_mobiledayssincefirstuse", StringType(), True),
    StructField("post_mobiledayssincelastuse", StringType(), True),
    StructField("post_mobiledevice", StringType(), True),
    StructField("post_mobilehourofday", StringType(), True),
    StructField("post_mobileinstalldate", StringType(), True),
    StructField("post_mobilelaunchnumber", StringType(), True),
    StructField("post_mobileltv", StringType(), True),
    StructField("post_mobilemessageid", StringType(), True),
    StructField("post_mobilemessageonline", StringType(), True),
    StructField("post_mobileosversion", StringType(), True),
    StructField("post_mobilepushoptin", BooleanType(), True),
    StructField("post_mobilepushpayloadid", StringType(), True),
    StructField("post_mobileresolution", StringType(), True),
    StructField("post_mvvar1", StringType(), True),
    StructField("post_mvvar2", StringType(), True),
    StructField("post_mvvar3", StringType(), True),
    StructField("post_page_event", ShortType(), True),
    StructField("post_page_event_var1", StringType(), True),
    StructField("post_page_event_var2", StringType(), True),
    StructField("post_page_event_var3", StringType(), True),
    StructField("post_page_type", StringType(), True),
    StructField("post_page_url", StringType(), True),
    StructField("post_pagename", StringType(), True),
    StructField("post_pagename_no_url", StringType(), True),
    StructField("post_partner_plugins", StringType(), True),
    StructField("post_persistent_cookie", StringType(), True),
    StructField("post_pointofinterest", StringType(), True),
    StructField("post_pointofinterestdistance", StringType(), True),
    StructField("post_product_list", StringType(), True),
    StructField("post_prop1", StringType(), True),
    StructField("post_prop2", StringType(), True),
    StructField("post_prop3", StringType(), True),
    StructField("post_prop4", StringType(), True),
    StructField("post_prop5", StringType(), True),
    StructField("post_prop6", StringType(), True),
    StructField("post_prop7", StringType(), True),
    StructField("post_prop8", StringType(), True),
    StructField("post_prop9", StringType(), True),
    StructField("post_prop10", StringType(), True),
    StructField("post_prop11", StringType(), True),
    StructField("post_prop12", StringType(), True),
    StructField("post_prop13", StringType(), True),
    StructField("post_prop14", StringType(), True),
    StructField("post_prop15", StringType(), True),
    StructField("post_prop16", StringType(), True),
    StructField("post_prop17", StringType(), True),
    StructField("post_prop18", StringType(), True),
    StructField("post_prop19", StringType(), True),
    StructField("post_prop20", StringType(), True),
    StructField("post_prop21", StringType(), True),
    StructField("post_prop22", StringType(), True),
    StructField("post_prop23", StringType(), True),
    StructField("post_prop24", StringType(), True),
    StructField("post_prop25", StringType(), True),
    StructField("post_prop26", StringType(), True),
    StructField("post_prop27", StringType(), True),
    StructField("post_prop28", StringType(), True),
    StructField("post_prop29", StringType(), True),
    StructField("post_prop30", StringType(), True),
    StructField("post_prop31", StringType(), True),
    StructField("post_prop32", StringType(), True),
    StructField("post_prop33", StringType(), True),
    StructField("post_prop34", StringType(), True),
    StructField("post_prop35", StringType(), True),
    StructField("post_prop36", StringType(), True),
    StructField("post_prop37", StringType(), True),
    StructField("post_prop38", StringType(), True),
    StructField("post_prop39", StringType(), True),
    StructField("post_prop40", StringType(), True),
    StructField("post_prop41", StringType(), True),
    StructField("post_prop42", StringType(), True),
    StructField("post_prop43", StringType(), True),
    StructField("post_prop44", StringType(), True),
    StructField("post_prop45", StringType(), True),
    StructField("post_prop46", StringType(), True),
    StructField("post_prop47", StringType(), True),
    StructField("post_prop48", StringType(), True),
    StructField("post_prop49", StringType(), True),
    StructField("post_prop50", StringType(), True),
    StructField("post_prop51", StringType(), True),
    StructField("post_prop52", StringType(), True),
    StructField("post_prop53", StringType(), True),
    StructField("post_prop54", StringType(), True),
    StructField("post_prop55", StringType(), True),
    StructField("post_prop56", StringType(), True),
    StructField("post_prop57", StringType(), True),
    StructField("post_prop58", StringType(), True),
    StructField("post_prop59", StringType(), True),
    StructField("post_prop60", StringType(), True),
    StructField("post_prop61", StringType(), True),
    StructField("post_prop62", StringType(), True),
    StructField("post_prop63", StringType(), True),
    StructField("post_prop64", StringType(), True),
    StructField("post_prop65", StringType(), True),
    StructField("post_prop66", StringType(), True),
    StructField("post_prop67", StringType(), True),
    StructField("post_prop68", StringType(), True),
    StructField("post_prop69", StringType(), True),
    StructField("post_prop70", StringType(), True),
    StructField("post_prop71", StringType(), True),
    StructField("post_prop72", StringType(), True),
    StructField("post_prop73", StringType(), True),
    StructField("post_prop74", StringType(), True),
    StructField("post_prop75", StringType(), True),
    StructField("post_purchaseid", StringType(), True),
    StructField("post_referrer", StringType(), True),
    StructField("post_s_kwcid", StringType(), True),
    StructField("post_search_engine", LongType(), True),
    StructField("post_socialaccountandappids", StringType(), True),
    StructField("post_socialassettrackingcode", StringType(), True),
    StructField("post_socialauthor", StringType(), True),
    StructField("post_socialcontentprovider", StringType(), True),
    StructField("post_socialfbstories", StringType(), True),
    StructField("post_socialfbstorytellers", StringType(), True),
    StructField("post_socialinteractioncount", StringType(), True),
    StructField("post_socialinteractiontype", StringType(), True),
    StructField("post_sociallanguage", StringType(), True),
    StructField("post_sociallatlong", StringType(), True),
    StructField("post_sociallikeadds", StringType(), True),
    StructField("post_socialmentions", StringType(), True),
    StructField("post_socialowneddefinitioninsighttype", StringType(), True),
    StructField("post_socialowneddefinitioninsightvalue", StringType(), True),
    StructField("post_socialowneddefinitionmetric", StringType(), True),
    StructField("post_socialowneddefinitionpropertyvspost", StringType(), True),
    StructField("post_socialownedpostids", StringType(), True),
    StructField("post_socialownedpropertyid", StringType(), True),
    StructField("post_socialownedpropertyname", StringType(), True),
    StructField("post_socialownedpropertypropertyvsapp", StringType(), True),
    StructField("post_socialpageviews", StringType(), True),
    StructField("post_socialpostviews", StringType(), True),
    StructField("post_socialpubcomments", StringType(), True),
    StructField("post_socialpubposts", StringType(), True),
    StructField("post_socialpubrecommends", StringType(), True),
    StructField("post_socialpubsubscribers", StringType(), True),
    StructField("post_socialterm", StringType(), True),
    StructField("post_socialtotalsentiment", StringType(), True),
    StructField("post_state", StringType(), True),
    StructField("post_survey", StringType(), True),
    StructField("post_t_time_info", StringType(), True),
    StructField("post_tnt", StringType(), True),
    StructField("post_tnt_action", StringType(), True),
    StructField("post_transactionid", StringType(), True),
    StructField("post_video", StringType(), True),
    StructField("post_videoad", StringType(), True),
    StructField("post_videoadinpod", StringType(), True),
    StructField("post_videoadlength", StringType(), True),
    StructField("post_videoadname", StringType(), True),
    StructField("post_videoadplayername", StringType(), True),
    StructField("post_videoadpod", StringType(), True),
    StructField("post_videochannel", StringType(), True),
    StructField("post_videochapter", StringType(), True),
    StructField("post_videocontenttype", StringType(), True),
    StructField("post_videolength", StringType(), True),
    StructField("post_videoname", StringType(), True),
    StructField("post_videoplayername", StringType(), True),
    StructField("post_videoqoebitrateaverageevar", StringType(), True),
    StructField("post_videoqoebitratechangecountevar", StringType(), True),
    StructField("post_videoqoebuffercountevar", StringType(), True),
    StructField("post_videoqoebuffertimeevar", StringType(), True),
    StructField("post_videoqoedroppedframecountevar", StringType(), True),
    StructField("post_videoqoeerrorcountevar", StringType(), True),
    StructField("post_videoqoetimetostartevar", StringType(), True),
    StructField("post_videosegment", StringType(), True),
    StructField("post_visid_high", StringType(), True),
    StructField("post_visid_low", StringType(), True),
    StructField("post_visid_type", ShortType(), True),
    StructField("post_zip", StringType(), True),
    StructField("prev_page", LongType(), True),
    StructField("product_list", StringType(), True),
    StructField("product_merchandising", StringType(), True),
    StructField("prop1", StringType(), True),
    StructField("prop2", StringType(), True),
    StructField("prop3", StringType(), True),
    StructField("prop4", StringType(), True),
    StructField("prop5", StringType(), True),
    StructField("prop6", StringType(), True),
    StructField("prop7", StringType(), True),
    StructField("prop8", StringType(), True),
    StructField("prop9", StringType(), True),
    StructField("prop10", StringType(), True),
    StructField("prop11", StringType(), True),
    StructField("prop12", StringType(), True),
    StructField("prop13", StringType(), True),
    StructField("prop14", StringType(), True),
    StructField("prop15", StringType(), True),
    StructField("prop16", StringType(), True),
    StructField("prop17", StringType(), True),
    StructField("prop18", StringType(), True),
    StructField("prop19", StringType(), True),
    StructField("prop20", StringType(), True),
    StructField("prop21", StringType(), True),
    StructField("prop22", StringType(), True),
    StructField("prop23", StringType(), True),
    StructField("prop24", StringType(), True),
    StructField("prop25", StringType(), True),
    StructField("prop26", StringType(), True),
    StructField("prop27", StringType(), True),
    StructField("prop28", StringType(), True),
    StructField("prop29", StringType(), True),
    StructField("prop30", StringType(), True),
    StructField("prop31", StringType(), True),
    StructField("prop32", StringType(), True),
    StructField("prop33", StringType(), True),
    StructField("prop34", StringType(), True),
    StructField("prop35", StringType(), True),
    StructField("prop36", StringType(), True),
    StructField("prop37", StringType(), True),
    StructField("prop38", StringType(), True),
    StructField("prop39", StringType(), True),
    StructField("prop40", StringType(), True),
    StructField("prop41", StringType(), True),
    StructField("prop42", StringType(), True),
    StructField("prop43", StringType(), True),
    StructField("prop44", StringType(), True),
    StructField("prop45", StringType(), True),
    StructField("prop46", StringType(), True),
    StructField("prop47", StringType(), True),
    StructField("prop48", StringType(), True),
    StructField("prop49", StringType(), True),
    StructField("prop50", StringType(), True),
    StructField("prop51", StringType(), True),
    StructField("prop52", StringType(), True),
    StructField("prop53", StringType(), True),
    StructField("prop54", StringType(), True),
    StructField("prop55", StringType(), True),
    StructField("prop56", StringType(), True),
    StructField("prop57", StringType(), True),
    StructField("prop58", StringType(), True),
    StructField("prop59", StringType(), True),
    StructField("prop60", StringType(), True),
    StructField("prop61", StringType(), True),
    StructField("prop62", StringType(), True),
    StructField("prop63", StringType(), True),
    StructField("prop64", StringType(), True),
    StructField("prop65", StringType(), True),
    StructField("prop66", StringType(), True),
    StructField("prop67", StringType(), True),
    StructField("prop68", StringType(), True),
    StructField("prop69", StringType(), True),
    StructField("prop70", StringType(), True),
    StructField("prop71", StringType(), True),
    StructField("prop72", StringType(), True),
    StructField("prop73", StringType(), True),
    StructField("prop74", StringType(), True),
    StructField("prop75", StringType(), True),
    StructField("purchaseid", StringType(), True),
    StructField("quarterly_visitor", ShortType(), True),
    StructField("ref_domain", StringType(), True),
    StructField("ref_type", ShortType(), True),
    StructField("referrer", StringType(), True),
    StructField("resolution", LongType(), True),
    StructField("s_kwcid", StringType(), True),
    StructField("s_resolution", StringType(), True),
    StructField("sampled_hit", StringType(), True),
    StructField("search_engine", LongType(), True),
    StructField("search_page_num", LongType(), True),
    StructField("secondary_hit", ShortType(), True),
    StructField("service", StringType(), True),
    StructField("socialaccountandappids", StringType(), True),
    StructField("socialassettrackingcode", StringType(), True),
    StructField("socialauthor", StringType(), True),
    StructField("socialcontentprovider", StringType(), True),
    StructField("socialfbstories", StringType(), True),
    StructField("socialfbstorytellers", StringType(), True),
    StructField("socialinteractioncount", StringType(), True),
    StructField("socialinteractiontype", StringType(), True),
    StructField("sociallanguage", StringType(), True),
    StructField("sociallatlong", StringType(), True),
    StructField("sociallikeadds", StringType(), True),
    StructField("socialmentions", StringType(), True),
    StructField("socialowneddefinitioninsighttype", StringType(), True),
    StructField("socialowneddefinitioninsightvalue", StringType(), True),
    StructField("socialowneddefinitionmetric", StringType(), True),
    StructField("socialowneddefinitionpropertyvspost", StringType(), True),
    StructField("socialownedpostids", StringType(), True),
    StructField("socialownedpropertyid", StringType(), True),
    StructField("socialownedpropertyname", StringType(), True),
    StructField("socialownedpropertypropertyvsapp", StringType(), True),
    StructField("socialpageviews", StringType(), True),
    StructField("socialpostviews", StringType(), True),
    StructField("socialpubcomments", StringType(), True),
    StructField("socialpubposts", StringType(), True),
    StructField("socialpubrecommends", StringType(), True),
    StructField("socialpubsubscribers", StringType(), True),
    StructField("socialterm", StringType(), True),
    StructField("socialtotalsentiment", StringType(), True),
    StructField("sourceid", LongType(), True),
    StructField("state", StringType(), True),
    StructField("stats_server", StringType(), True),
    StructField("t_time_info", StringType(), True),
    StructField("tnt", StringType(), True),
    StructField("tnt_action", StringType(), True),
    StructField("tnt_post_vista", StringType(), True),
    StructField("transactionid", StringType(), True),
    StructField("truncated_hit", StringType(), True),
    StructField("ua_color", StringType(), True),
    StructField("ua_os", StringType(), True),
    StructField("ua_pixels", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("user_hash", LongType(), True),
    StructField("user_server", StringType(), True),
    StructField("userid", LongType(), True),
    StructField("username", StringType(), True),
    StructField("va_closer_detail", StringType(), True),
    StructField("va_closer_id", ShortType(), True),
    StructField("va_finder_detail", StringType(), True),
    StructField("va_finder_id", ShortType(), True),
    StructField("va_instance_event", ShortType(), True),
    StructField("va_new_engagement", ShortType(), True),
    StructField("video", StringType(), True),
    StructField("videoad", StringType(), True),
    StructField("videoadinpod", StringType(), True),
    StructField("videoadlength", StringType(), True),
    StructField("videoadname", StringType(), True),
    StructField("videoadplayername", StringType(), True),
    StructField("videoadpod", StringType(), True),
    StructField("videochannel", StringType(), True),
    StructField("videochapter", StringType(), True),
    StructField("videocontenttype", StringType(), True),
    StructField("videolength", StringType(), True),
    StructField("videoname", StringType(), True),
    StructField("videoplayername", StringType(), True),
    StructField("videoqoebitrateaverageevar", StringType(), True),
    StructField("videoqoebitratechangecountevar", StringType(), True),
    StructField("videoqoebuffercountevar", StringType(), True),
    StructField("videoqoebuffertimeevar", StringType(), True),
    StructField("videoqoedroppedframecountevar", StringType(), True),
    StructField("videoqoeerrorcountevar", StringType(), True),
    StructField("videoqoetimetostartevar", StringType(), True),
    StructField("videosegment", StringType(), True),
    StructField("visid_high", StringType(), True),
    StructField("visid_low", StringType(), True),
    StructField("visid_new", StringType(), True),
    StructField("visid_timestamp", LongType(), True),
    StructField("visid_type", ShortType(), True),
    StructField("visit_keywords", StringType(), True),
    StructField("visit_num", LongType(), True),
    StructField("visit_page_num", LongType(), True),
    StructField("visit_referrer", StringType(), True),
    StructField("visit_search_engine", LongType(), True),
    StructField("visit_start_page_url", StringType(), True),
    StructField("visit_start_pagename", StringType(), True),
    StructField("visit_start_time_gmt", LongType(), True),
    StructField("weekly_visitor", ShortType(), True),
    StructField("yearly_visitor", ShortType(), True),
    StructField("zip", StringType(), True)
]

schema = StructType(fields)

# Build join lookup table
lookup_tables = [
    ('browser', 'browser'),
    ('color_depth', 'color'),
    ('connection_type', 'connection_type'),
    ('country', 'country'),
    #'event', # Event list contains a list of events. e.g., 100,101,102,103,104,105,106,107,108,109,119,120,121,122,141,150
    ('javascript_version', 'javascript'),
    ('language', 'language'),
    ('operating_systems', 'os'),
    ('plugins', 'plugins'),
    ('referrer_type', 'ref_type'),
    ('referrer_type', 'ref_domain'),
    ('resolution', 'resolution'),
    ('search_engine', 'search_engine')
]

# Initialise boto3
s3_client = boto3.client('s3')


logger.info('Reading the job arguments to find the source bucket.')

# Extract bucket from source uri
parsed = urlparse(args['s3source'])
source_bucket = parsed.netloc


def extract_files(bytes):
    tar = tarfile.open(fileobj=BytesIO(bytes), mode="r:gz")
    return [(x.name, tar.extractfile(x).read()) for x in tar if x.isfile()]

def row_parser(row):
    arr = row.split('\t')
    if len(arr) == 2:
        return (int(arr[0]), arr[1])
    else:
        return (None, None)

def copy_to_processed(key, bucket=source_bucket):
    kwargs = {
        'CopySource': {
            'Bucket': bucket,
            'Key': key},
        'Bucket': bucket,
        'Key': key.replace("incoming/", "processed/", 1),
        'ContentType': 'application/x-gzip',
        'Metadata': {
            'ProcessDate': str(datetime.now())
        },
        'MetadataDirective': 'REPLACE',
        'StorageClass': 'STANDARD_IA'
    }
    response = s3_client.copy_object(**kwargs)
    if 'ServerSideEncryption' not in response:
        logger.warn('Bucket policy does not encrypt the file by default. File key: ', key)
    return response

def delete_files(keys, bucket=source_bucket):
    '''Deletes the list of input files
    '''
    delete_result = s3_client.delete_objects(
        Bucket=source_bucket,
        Delete={
            'Objects': [{'Key': key} for key in keys[:1000]]
        }
    )
    if len(keys) > 1000:
        delete2_result = delete_files(keys=keys[1000:], bucket=source_bucket)
        delete_result['Deleted'] += delete2_result['Deleted']
        delete_result['Errors'] = delete_result.get('Errors', list()) + \
            delete2_result.get('Errors', list())
    return delete_result

def move_files_to_processed(keys):
    '''Copies the files to the processed folder
    and deletes them from the original location
    '''
    with closing(Pool(processes=5)) as pool:
        copy_result = pool.map(copy_to_processed, keys)
    copy_failures = len(filter(lambda x: not 200 <= x['ResponseMetadata']['HTTPStatusCode'] <= 299 , copy_result))
    if copy_failures == 0:
        delete_files(keys)
    else:
        raise Exception('Could not copy the files to the processed folder. ' + \
            'Aborted the delete operation.')


# Define the schema of lookup tables
lookup_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
])

# Get all the incoming files
def get_all_files(s3_client, bucket, prefix=''):
    '''Returns a set contianing all the files in the specified
    bucket and prefix.
    '''
    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    is_truncated = True
    all_files = set()
    while is_truncated:
        response = s3_client.list_objects_v2(**kwargs)
        if response['IsTruncated']:
            kwargs['ContinuationToken'] = response['NextContinuationToken']
        else:
            is_truncated = False
        if 'Contents' in response.keys():
            all_files = all_files.union({obj['Key'] for obj in response['Contents']})
    return all_files

def s3_format_path(key, bucket=source_bucket):
    '''Formats a given key and a bucket name to conform
    to s3:// URI format
    '''
    return  's3://' + bucket + '/' + key

def get_lookup_file(files):
    '''Returns the most recent lookup file that exists in the
    input argument.
    '''
    lfiles = filter(lambda x: x.endswith('lookup_data.tar.gz'), all_files)
    return s3_format_path(max(lfiles))

logger.info('Getting the list of files available in the source S3 bucket.')

all_files = get_all_files(s3_client=s3_client, bucket=source_bucket, prefix='incoming/')

lookup_file = get_lookup_file(all_files)

lookup_tables_rdd = (spark.sparkContext.binaryFiles(lookup_file)
    .flatMapValues(extract_files)
    .map(lambda row: (row[1][0], row[1][1].decode('utf-8', 'ignore'))))

lookup_tables_rdd = lookup_tables_rdd.repartition(100)
lookup_tables_df = dict()
for lookup in lookup_tables:
    logger.info('Reading and caching lookup file ' + lookup[0] + '.tsv')
    lookup_rdd = (lookup_tables_rdd
                .filter(lambda x: x[0] == lookup[0] + '.tsv')
                .flatMap(lambda x: (x[1].split('\n')))
                .map(row_parser)
                .filter(lambda x: x[0] != None))
    df_lookup = spark.createDataFrame(lookup_rdd, schema=lookup_schema)
    lookup_tables_df[lookup] = df_lookup.repartition(100)
    lookup_tables_df[lookup].cache()

logger.info('Finished reading lookup files.')

def batch_data_files_by_date(files):
    '''Returns a dictionary with days as key and
    list of files belonging to that day as value.
    '''
    file_batch = dict()
    for key in data_files:
        date = key.split('-')[-2].split('_')[1]
        file_batch[date] = file_batch.get(date, list()) + [key]
    return file_batch

data_files = filter(lambda x: x.endswith('.tsv.gz'), all_files)

logger.info('Batching the data files into dates.')
file_batch = batch_data_files_by_date(data_files)

for date in sorted(file_batch.keys())[:-1]:
    files_to_process = map(s3_format_path, file_batch[date])
    logger.info('Processing the following files for date ' + date + '.\n\t' + '\n\t'.join(sorted(files_to_process)))
    df0 = spark \
        .read.format("com.databricks.spark.csv") \
        .option("quote", "\"") \
        .option("delimiter", u'\u0009') \
        .option("charset", 'utf-8') \
        .schema(schema) \
        .load(files_to_process) \
        .dropna(how = 'all')
    df1 = df0.withColumn('date', df0.date_time.cast('date'))
    df2 = df1
    for i, lookup in enumerate(lookup_tables):
        lookup_table_df = lookup_tables_df[lookup]
        if not i % 4:
            df2 =  df2.repartition(100)
        df2 = (df2
            .join(broadcast(lookup_table_df), col(lookup[1]) == lookup_table_df.id, 'left')
            .drop('id')
            .withColumnRenamed(lookup[1], lookup[1] + '_id')
            .withColumnRenamed('name', lookup[1]))
    ds2 = df2.repartition(4).write.format("parquet").partitionBy("date").mode("append").save(args['s3target'])
    move_files_to_processed(file_batch[date])

logger.info('Removing the non-data files such as manifest and lookup tables.')
not_data_files = filter(lambda x: not x.endswith('.tsv.gz'), all_files)
move_files_to_processed(not_data_files)

logger.info('Finished processing the data. Commiting the job.')

job.commit()
