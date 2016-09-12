Method Description:

Map: Reads lines, extracts the required fields.
impressions, clicks, conversions, campaign_id

and outputs (Key=campaign_id, Value=impressions,clicks,conversions)

Reduce: For a key, goes over an iterable list and sums up the corresponding fields and outputs (Key=campaign_id, Value=sum_impressions,sum_clicks,sum_conversions)

Running Instructions
Place the input data file in the same folder as the jar. Make sure that there is nothing named output on the same folder

then run the command: 

java -jar mp11.jar cs511_data output



The output file will be generated in the output folder.

The running time is high since you will be running it from a jar. It is about 1 min and 10 seconds on my machine. If I run it as an eclipse project, it takes about 5-6 secs for the output folder to get generated. 