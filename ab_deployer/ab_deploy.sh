# Validating if output file has been passed
if [ -z "$1" ]
then
	echo "ERROR: OUTPUT FILE NAME REQUIRED."
	echo "ab_deployer <outputFile> <configFile> <server>"
	exit 1
fi

# Validating if a configuration file has been passed
if [ -z "$2" ]
then
	echo "ERROR: CONFIGURATION FILE NAME REQUIRED."
	exit 1
fi

# Validating if a server name has been provided
if [ -z "$3" ]
then
	echo "ERROR: SERVER NAME REQUIRED."
	exit 1
fi

if [ -z "$4" ]
then
	echo "ERROR: OPERATION REQUIRED."
	exit 1
fi

# Defining variables needed
CONF_FILE=$2
OUTPUT_FILE=$1
DEFAULT_EXEC_NUM=1
LINES_TAKEN=100
SLEEP_TIME=5
SERVER=$3
COOR1=$[ ( $RANDOM % 180 )  + 1 ]
COOR2=$[ ( $RANDOM % 180 )  + 1 ]

# Defining operation requests
READ_REQ="{action:1413,scenarioId:1,lat:-25,lng:-42,radius:1000}"

WRITE_REQ="%7Baction:1310,accessToken=%22AAFFRRDDFFSFF%22,geotag:%7B%22title%22:%22teste%22,%22text%22:%22teste%22,%22infobox%22:%7B%22title%22:%22teste%22,%22text%22:%22asfdasfasdfsdafasdf%22%7D,%22location%22:%7B%22lng%22:${COOR1},%22lat%22:${COOR2}%7D,%22image%22:%7B%22id%22:%220%22,%22mime%22:%22%22%7D,%22type%22:%22text%22,%22uncodedTitle%22:%22teste%22,%22uncodedText%22:%22teste%22,%22fbUserId%22:123456789,%22fbChecked%22:false,%22userId%22:1,%22scenarioId%22:%221%22,%22id%22:%22%22,fbEmail:%22marta.silva@habanero.com%22,fbFstName=%22Marta%22,fbLstName=%22Silva%22,fbUserName=%22tt%22%7D%7D"

if [ $4 == "read" ]
then
	REQUEST=${READ_REQ}
fi
if [ $4 == "write" ]
then
	REQUEST=${WRITE_REQ}
fi

LOADBALANCER=paprikaloadbalancer-1168239303.us-east-1.elb.amazonaws.com

NUMBER=$[ ( $RANDOM % 100 )  + 1 ]

#REQUEST=${WRITE_REQ}

# Executing Apache AB command for a number of times
echo "==============================="
echo "= Executing Apache AB command ="
echo "==============================="

iCnt=0
# Obtaining configuration to run Apache AB tool
while read line
do
	let iCnt=iCnt+1
	# Starting
	echo "Time number ${iCnt} "
	NUM_OPS=`echo $line | awk '{x=$1}END{print x}'`
	NUM_THREADS=`echo $line | awk '{x=$2}END{print x}'`
	echo "concurrentOps ${NUM_THREADS} totalReqs ${NUM_OPS}"

	ab -A tomcat:tomcat -b 1024 -c ${NUM_THREADS}  -n ${NUM_OPS} -r -v 4 http://${SERVER}:8080/servletMobile/mobile?param=${REQUEST} 2>&1 | tail -${LINES_TAKEN} >> ${OUTPUT_FILE}

	# Separating outputs
	echo "===============================================================================" >> $OUTPUT_FILE
	echo "===============================================================================" >> $OUTPUT_FILE

	# Sleeping
	echo "Giving the server ${SLEEP_TIME} seconds of peace"
	sleep ${SLEEP_TIME}

done < "${CONF_FILE}"

