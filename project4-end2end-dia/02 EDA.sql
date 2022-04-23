-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Ethereum Blockchain Data Analysis - <a href=https://github.com/blockchain-etl/ethereum-etl-airflow/tree/master/dags/resources/stages/raw/schemas>Table Schemas</a>
-- MAGIC - **Transactions** - Each block in the blockchain is composed of zero or more transactions. Each transaction has a source address, a target address, an amount of Ether transferred, and an array of input bytes. This table contains a set of all transactions from all blocks, and contains a block identifier to get associated block-specific information associated with each transaction.
-- MAGIC - **Blocks** - The Ethereum blockchain is composed of a series of blocks. This table contains a set of all blocks in the blockchain and their attributes.
-- MAGIC - **Receipts** - the cost of gas for specific transactions.
-- MAGIC - **Traces** - The trace module is for getting a deeper insight into transaction processing. Traces exported using <a href=https://openethereum.github.io/JSONRPC-trace-module.html>Parity trace module</a>
-- MAGIC - **Tokens** - Token data including contract address and symbol information.
-- MAGIC - **Token Transfers** - The most popular type of transaction on the Ethereum blockchain invokes a contract of type ERC20 to perform a transfer operation, moving some number of tokens from one 20-byte address to another 20-byte address. This table contains the subset of those transactions and has further processed and denormalized the data to make it easier to consume for analysis of token transfer events.
-- MAGIC - **Contracts** - Some transactions create smart contracts from their input bytes, and this smart contract is stored at a particular 20-byte address. This table contains a subset of Ethereum addresses that contain contract byte-code, as well as some basic analysis of that byte-code. 
-- MAGIC - **Logs** - Similar to the token_transfers table, the logs table contains data for smart contract events. However, it contains all log data, not only ERC20 token transfers. This table is generally useful for reporting on any logged event type on the Ethereum blockchain.
-- MAGIC 
-- MAGIC ### Rubric for this module
-- MAGIC Answer the quetions listed below.

-- COMMAND ----------

-- MAGIC %run ./includes/utilities

-- COMMAND ----------

-- MAGIC %run ./includes/configuration

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Grab the global variables
-- MAGIC wallet_address,start_date = Utils.create_widgets()
-- MAGIC print(wallet_address,start_date)
-- MAGIC spark.conf.set('wallet.address',wallet_address)
-- MAGIC spark.conf.set('start.date',start_date)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC USE ethereumetl;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What is the maximum block number and date of block in the database 1

-- COMMAND ----------

-- TBD

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: At what block did the first ERC20 transfer happen? 2

-- COMMAND ----------

-- TBD

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: How many ERC20 compatible contracts are there on the blockchain? 3

-- COMMAND ----------

-- TBD

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Q: What percentage of transactions are calls to contracts 4

-- COMMAND ----------

-- TBD
SELECT *
FROM Transactions
WHERE gas_price == 0

-- COMMAND ----------

SELECT COUNT(*)
FROM Transactions
WHERE gas_price == 0
-- Total calls (no cost to call) = 37795

-- COMMAND ----------

SELECT *
FROM Transactions
WHERE gas == 0

-- COMMAND ----------

SELECT COUNT(*)/177974618
FROM Transactions
WHERE gas_price == 0
-- Fraction of calls to total transactions = 0.0002123617425042036

-- COMMAND ----------

SELECT COUNT(*)
FROM Transactions
-- Total Transactions = 177974618

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What are the top 100 tokens based on transfer count? 5

-- COMMAND ----------

-- TBD

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What fraction of ERC-20 transfers are sent to new addresses 6
-- MAGIC (i.e. addresses that have a transfer count of 1 meaning there are no other transfers to this address for this token this is the first)

-- COMMAND ----------

Select Count(*)
FROM Token_Transfers
-- Total # of transfers = 922029708

-- COMMAND ----------

-- TBD
SELECT *
FROM Token_Transfers
WHERE value == 1

-- There are multiple lines of the same token address going to the same to_address is this just an amount of token?

-- COMMAND ----------

SELECT COUNT(*)/922029708
FROM Token_Transfers
WHERE value == 1
-- Number of transactions with transfer count (value) = 1/ total number of transactions
-- fraction of ERC-20 transfers are sent to new addresses = 0.0019443831195946671

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: In what order are transactions included in a block in relation to their gas price? 7
-- MAGIC - hint: find a block with multiple transactions 

-- COMMAND ----------

-- TBD

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What was the highest transaction throughput in transactions per second? 8
-- MAGIC hint: assume 15 second block time

-- COMMAND ----------

-- TBD

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What is the total Ether volume?
-- MAGIC Note: 1x10^18 wei to 1 eth and value in the transaction table is in wei

-- COMMAND ----------

-- TBD

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What is the total gas used in all transactions?

-- COMMAND ----------

-- TBD
SELECT SUM(gas)
FROM Transactions
-- Total gas used in all transactions = 26962124687146
-- gas = "gas provided by the sender"

-- COMMAND ----------

SELECT SUM(gas_used)
FROM Receipts
-- Total gas used in all transactions = 93783326139907
-- gas_used = "The amount of gas used by this specific transaction alone"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: Maximum ERC-20 transfers in a single transaction

-- COMMAND ----------

-- TBD

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: Token balance for any address on any date?

-- COMMAND ----------

-- TBD

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Viz the transaction count over time (network use)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC df = sqlContext.sql("SELECT transaction_count, timestamp FROM blocks")
-- MAGIC timedf = df.select("transaction_count", from_unixtime(col("timestamp"),"MM-dd-yyyy").alias("date"))
-- MAGIC 
-- MAGIC time = timedf.select("transaction_count", "date").groupBy('date').count()
-- MAGIC display(time)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Viz ERC-20 transfer count over time
-- MAGIC interesting note: https://blog.ins.world/insp-ins-promo-token-mixup-clarified-d67ef20876a3

-- COMMAND ----------

-- TBD


-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Return Success
-- MAGIC dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
