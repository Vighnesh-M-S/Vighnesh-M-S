from pyspark import Sparkcontext
import pyspark.sql.functions as f
from pyspark.streaming import StreamingContext
from pyspark.sql import Sparksession
import pyspark.sql.types as tp
def preprocess (res_dd):
	res_dd.show()
	df_crime=res_dd.map (lambda line: [x.strip('}') for x in next(reader([line]))])
        #get header
        df crime. show)
       
