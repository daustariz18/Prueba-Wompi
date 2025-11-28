# Reto Práctico Wompi – Ingeniero de Datos

Este repositorio contiene la solución al reto práctico de la prueba técnica para Ingeniero de Datos en Wompi. El proyecto implementa un proceso de análisis y transformación de un archivo JSONL con transacciones, utilizando Python y PySpark para generar una vista agregada por BIN y fecha. El resultado final se almacena en formato Parquet para facilitar su consulta y análisis posterior.

## Objetivo del reto

De acuerdo con las instrucciones del documento oficial, el script debe:

- Leer el archivo `transactions_50k.jsonl`.
- Limpiar y transformar los datos relevantes.
- Agregar la información por BIN y fecha, generando:
  - Cantidad total de transacciones aprobadas.
  - Monto total aprobado.
- Exportar el resultado en formato Parquet.

## Estructura del proyecto

```
Prueba-Wompi/
├── src/
│   └── spark_transactions.py
├── output/
│   └── resumen_transacciones.parquet/
├── README.md
└── requirements.txt
```

## Instrucciones de ejecución

### 1. Instalar dependencias

```
pip install -r requirements.txt
```

### 2. Ejecutar el script

```
python src/spark_transactions.py
```

### 3. Consultar el resultado

El archivo generado se encuentra en:

```
output/resumen_transacciones.parquet/
```

## Detalles de la solución

- El procesamiento se realiza con PySpark en modo local (`local[*]`), sin necesidad de un clúster.
- Se aplican transformaciones para estandarizar los tipos de datos y seleccionar únicamente las columnas necesarias.
- Las agregaciones se ejecutan mediante funciones nativas de Spark (`count`, `sum`).
- Se utiliza modo de sobrescritura garantizando idempotencia en la generación del resultado.
- El archivo Parquet resultante es compatible con lectores como PySpark, Pandas, Amazon Athena y Amazon Redshift Spectrum.

## Dependencias

```
pyspark==3.5.0
pandas
pyarrow
```

## Esquema final del archivo de salida

- bin  
- date  
- transaction_count  
<<<<<<< HEAD
- total_approved_amount
=======
- total_approved_amount
>>>>>>> 9348dffb4f6adc785e62673738318832794d7867
