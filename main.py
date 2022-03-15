from pyspark.sql import SparkSession
from pyspark import *
from pyspark.sql.functions import weekofyear, year, broadcast
import pyspark.sql.functions as F
import argparse
import sys, getopt, os


def main():
    """
    Main function is for parsing arguments
    """
    parser = argparse.ArgumentParser("Program to process yelp data and return output JSON files")
    parser.add_argument("-i", required=True, help="Please provide input files directory")
    parser.add_argument("-o", required=True, help="Please provide output files directory")
    args = parser.parse_args()
    
    return args


def read_files(spark, input_path):
    """
    Take path of input directory to read JSON files and creates dataframe
    parameters :
        input_path : String
    return :
        dataframes_list : list(dataframe)
    """
    # assign dataset names
    list_of_names = ['business', 'checkin', 'review', 'tip', 'user']
    
    # create empty list
    dataframes_list = []
    
    input_dir = os.getcwd() + '/' + input_path
    
    for x in range(len(list_of_names)):
        temp_df = spark.read.json(input_dir+"/yelp_academic_dataset_"+list_of_names[x]+".json")
        dataframes_list.append(temp_df)
    
    return dataframes_list[0], dataframes_list[1], dataframes_list[2], dataframes_list[3], dataframes_list[4]


def prepare_dataset(business_df, checkin_df, review_df):
    """
    Take required df as input and process them as per the requirement to produce final df
    parameters :
        business_df : Dataframe
        checkin_df : Dataframe
        review_df : Dataframe
    return :
        stars_weekly : Dataframe
        total_checkin_cases : Dataframe
    """
    
    # Selected releavant columns from the DF
    business_sel = business_df.select('business_id', 'name', 'review_count', 'stars')
                        #persist(StorageLevel.DISK_ONLY)
    review_df = review_df.repartition('business_id')
    review_sel = review_df.\
                    select('business_id', 'date', 'stars').\
                    withColumn('year', year('date')).\
                    withColumn('week', weekofyear('date')).persist(StorageLevel.MEMORY_AND_DISK)
    
    # Grouped review DF to get stars on weekly basis
    grouped_col = ['business_id', 'year', 'week']
    review_grouped = review_sel.groupBy(grouped_col).\
                    agg(F.round(F.avg('stars'),1).alias('weekly_stars')).\
                    drop('year', 'week')
    
    # Joined grouped review DF with business to get all the relevant fields from the DF
    stars_weekly = review_grouped.join(broadcast(business_df), "business_id")
    
    # Joined business with checnkin DF to populate with no of checkins against each business
    total_checkins = business_sel.join(checkin_df, "business_id", "left").\
                withColumn('no_of_checkins', F.size(F.split("date",","))).persist(StorageLevel.MEMORY_AND_DISK)

    # Set number of checkins to Zero for business with no checkin dates
    total_checkins_case = total_checkins.withColumn('no_of_checkins', F.when(F.col('date').isNotNull(),
                                            F.col('no_of_checkins')).otherwise(0)).drop('date')
    
    return stars_weekly, total_checkins_case


def write_files(output_path, stars_weekly, total_checkins_case):
    """
    This function helps in writing final df in output file at provided output location
    parameters :
        output_path : String
        stars_weekly : Dataframe
        total_checkins_case : Dataframe
    return :
        null
    """
    output_dir = os.getcwd() + '/' + output_path
    stars_weekly.coalesce(1).write.mode('overwrite').json(output_dir+"/starsWeekly")
    total_checkins_case.coalesce(1).write.mode('overwrite').json(output_dir+"/totalCheckins")
    
    return


if __name__ == "__main__":
    
    # Parsing arguments
    args = main()
    
    # Creating spark session
    spark = SparkSession.builder.getOrCreate()
    
    # Creating df by reading JSON files from provided input location
    business_df, checkin_df, review_df, tip_df, user_df = read_files(spark, args.i)
    
    # Transform input df to produce final df
    stars_weekly, total_checkins_case = prepare_dataset(business_df, checkin_df, review_df)
    
    # Save final df at output location in JSON format
    write_files(args.o, stars_weekly, total_checkins_case)
