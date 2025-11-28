Solución al reto práctico de la prueba técnica para Ingeniero de Datos en Wompi. Este proyecto procesa un archivo JSONL con transacciones utilizando Python y PySpark, genera una vista agregada por BIN y fecha y almacena el resultado final en formato Parquet.

Objetivo del reto

Según las instrucciones del documento oficial, el script debe:

* Leer el archivo transactions_50k.jsonl.

* Limpiar y transformar los datos.

* Agregar la información por BIN y fecha, generando:

* Cantidad total de transacciones aprobadas.

* Monto total aprobado.

* Guardar el resultado en formato Parquet para su posterior análisis.

Estructura del proyecto

Prueba-Wompi/
├── src/spark_transactions.py
├── output/resumen_transacciones.parquet/
├── README.md
└── requirements.txt

Instrucciones de ejecución
1. Instalar dependencias
   pip install -r requirements.txt
2. Ejecutar el script
   python src/spark_transactions.py
3. Resultado
   output/resumen_transacciones.parquet

Detalles de la solución

* Se ejecuta PySpark en modo local (local[*]), permitiendo correr el proceso sin un clúster.

* Se aplican transformaciones para estandarizar tipos de datos y extraer las columnas necesarias.

* La agregación se realiza mediante funciones de conteo y suma.

* La escritura en Parquet se realiza en modo sobrescritura para garantizar idempotencia.

* El resultado es compatible con lectores Parquet como PySpark, Pandas, Athena o Redshift Spectrum.

Dependencias
pyspark==3.5.0
pandas
pyarrow

Esquema final del archivo de salida

* bin
* date
* transaction_count
* total_approved_amount
   
