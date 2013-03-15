# Validating if output file has been passed
if [ -z "$1" ]
then
	echo "ERROR: OUTPUT FILE NAME REQUIRED."
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

# Defining variables needed
CONF_FILE=$2
OUTPUT_FILE=$1
DEFAULT_EXEC_NUM=1
LINES_TAKEN=100
SLEEP_TIME=10
SERVER=$3

# Executing Apache AB command for a number of times
echo "Executing Apache AB command for ${EXEC_NUM}"

iCnt=0
# Obtaining configuration to run Apache AB tool
while read line
do
	let iCnt=iCnt+1
	# Sleeping
	echo "Giving the server ${SLEEP_TIME} of peace"
	sleep ${SLEEP_TIME}
	# Starting
	echo "Time number ${iCnt} "
	NUM_OPS=`echo $line | awk '{x=$1}END{print x}'`
	NUM_THREADS=`echo $line | awk '{x=$2}END{print x}'`
	echo "concurrentOps ${NUM_THREADS} totalReqs ${NUM_OPS}"

	ab -A tomcat:tomcat -b 1024 -c ${NUM_THREADS}  -C 'scenarioId=1' -C 'radius=10000' -C 'll=-22.920734855121413,-43.2698221941406'  -n ${NUM_OPS} -r -v 4 http://${SERVER}:8080/paprika.json 2>&1 | tail -${LINES_TAKEN} >> ${OUTPUT_FILE}

	# Separating outputs
	echo "===============================================================================" >> $OUTPUT_FILE
	echo "===============================================================================" >> $OUTPUT_FILE
done < "${CONF_FILE}"

