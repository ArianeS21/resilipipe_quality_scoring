prepare:
	./scripts/prepare.sh
submit_spark:
	./scripts/submit_spark.sh ${OBJECT_PATH} ${MAX_MINUTES} spark.py
