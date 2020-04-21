youtubedata= LOAD '/project_raj/data/project_raj/USvideos.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER')
AS (video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,thumbnail_link,comments_disabled,ratings_disabled,video_error_or_removed,description
);
videogrp = GROUP youtubedata by video_id;
videodislikes = FOREACH videogrp GENERATE group, AVG(youtubedata.dislikes) as average;
orderLikes = ORDER videodislikes  by average DESc;
top25orderLikes = LIMIT orderLikes  10;
store top25orderLikes  INTO '/Project/pig_output/output_query3' USING PigStorage(':');

