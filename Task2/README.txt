Method Description:

Map: Reads lines, sets empty values as “null”, extracts the required fields.
ymdh, user_id and click.

If click is 1 and user_id is not null, reads the total number of milliseconds since a particular fixed date in 1970, for the ymdh object treated as a Java ‘Date’. Outputs two key value pairs for each entry,

(Key=milliseconds, Value=ymdh,user_id)
(Key=milliseconds-1000, Value=ymdh,user_id)

this makes sure that entries with same time or that are within 1000 milliseconds within each other are are sent with the same key. The same entry is effectively send twice, but duplicates will be treated later. 

Reduce: For a key, goes over an iterable list and puts the different entries at that time(key), and puts them in a hashmap (key=user_id,value=ymdh). Takes all the keys, sorts them and then outputs, (key=ymdh, value=user_id,user_id) for all possible pairs from the sorted user_id list such that the smaller one is always the first in the pair.

This will generate duplicate entries since for the entries with same time, two keys are generated initially, this needs to be treated which is done using subsequent steps.

Map2: This just outputs the value it receives as the key. value is not important. 

Reduce2: This just outputs the key it receives as is, thereby removing the duplicates. 

Running Instructions:
Place the input data file in the same folder as the jar. Make sure that there is nothing named output or intermediate_output in the same folder

then run the command: 
java -jar mp12.jar cs511_data output


The output file will be generated in the output folder.

The running time is high since you will be running it from a jar. It is about 1 min and 12 seconds on my machine. If I run it as an eclipse project, it takes about 4-5 secs for the output folder to get generated.