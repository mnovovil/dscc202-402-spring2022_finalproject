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
-- MAGIC print(wallet_address, start_date)
-- MAGIC spark.conf.set('wallet.address', wallet_address)
-- MAGIC spark.conf.set('start.date', start_date)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC USE ethereumetl;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What is the maximum block number and date of block in the database

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql("""SELECT number, timestamp FROM blocks 
-- MAGIC     WHERE number IN (SELECT MAX(number) FROM blocks)"""))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: At what block did the first ERC20 transfer happen?

-- COMMAND ----------

SELECT number FROM blocks 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC sql_statement = """
-- MAGIC SELECT MIN(block_number) FROM token_transfers
-- MAGIC """
-- MAGIC df = spark.sql(sql_statement)
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: How many ERC20 compatible contracts are there on the blockchain?

-- COMMAND ----------

-- This assumes that the token addresses
-- in the token_transfer table are all 
-- ERC20 contract addresses
SELECT 
  COUNT(DISTINCT token_address)
FROM token_transfers

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Q: What percentage of transactions are calls to contracts

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## NO IDEA WHAT IN THE HELL a CALLS to CONTRACTS IS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What are the top 100 tokens based on transfer count?

-- COMMAND ----------

SELECT
  token_address, COUNT(transaction_hash) transaction_count
FROM token_transfers
GROUP BY token_address
ORDER BY transaction_count DESC
LIMIT 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What fraction of ERC-20 transfers are sent to new addresses
-- MAGIC (i.e. addresses that have a transfer count of 1 meaning there are no other transfers to this address for this token this is the first)

-- COMMAND ----------

SELECT token_address, from_address, Count(*)
FROM token_transfers
group by token_address, from_address
HAVING count(*) = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: In what order are transactions included in a block in relation to their gas price?
-- MAGIC - hint: find a block with multiple transactions 
-- MAGIC 
-- MAGIC ## A: The order of the transaction included in a block are in gas price descending order.

-- COMMAND ----------

/*
-- Find a block number with more than 1 transaction in the
-- last partition of block table
SELECT number, transaction_count
FROM blocks
WHERE start_block>=14030000 start_block>=14030000 and transaction_count > 1
LIMIT 10
*/

-- List all 155 transactions in this specific block
-- The transactions look to be ordered with gas price in decending order
SELECT 
  hash, block_number, transaction_index, gas_price 
FROM transactions 
WHERE start_block>=14030000 and block_number = 14030401

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What was the highest transaction throughput in transactions per second?
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

-- Total gas used in all transactions = 93783326139907
-- gas_used = "The amount of gas used by this specific transaction alone"
SELECT SUM(gas_used)
FROM Receipts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: Maximum ERC-20 transfers in a single transaction

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC sql_statement = """
-- MAGIC SELECT block_number, COUNT(hash) FROM transactions
-- MAGIC     GROUP BY block_number
-- MAGIC         ORDER BY block_number DESC
-- MAGIC             LIMIT 1
-- MAGIC """
-- MAGIC df = spark.sql(sql_statement)
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: Token balance for any address on any date?

-- COMMAND ----------

-- TBD

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Viz the transaction count over time (network use)

-- COMMAND ----------

-- TBD

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Viz ERC-20 transfer count over time
-- MAGIC interesting note: https://blog.ins.world/insp-ins-promo-token-mixup-clarified-d67ef20876a3

-- COMMAND ----------

SELECT
  token_address, from_address, to_address, value, block_number, timestamp, CAST((timestamp/1e6) AS TIMESTAMP), transaction_count
FROM token_transfers TT
LEFT JOIN blocks B ON B.number = TT.block_number
WHERE token_address = "" AND CAST((timestamp/1e6) AS TIMESTAMP) >= '2021-12-25'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Return Success
-- MAGIC dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
