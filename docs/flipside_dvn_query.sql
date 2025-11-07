-- Deutsche Telekom DVN participation monitoring (Base -> Ethereum)
WITH deutsche_telekom_dvn_txs AS (
  SELECT DISTINCT
    tx_hash                                   AS dvn_tx_hash,
    block_number                              AS dvn_block_number,
    block_timestamp                           AS dvn_timestamp,
    decoded_log:requiredDVNs                  AS required_dvns,
    decoded_log:optionalDVNs                  AS optional_dvns,
    decoded_log:fees                          AS dvn_fees
  FROM base.core.ez_decoded_event_logs
  WHERE LOWER(contract_address) = LOWER('0xb5320b0b3a13cc860893e2bd79fcd7e13484dda2')
    AND event_name = 'DVNFeePaid'
    AND block_timestamp BETWEEN '2025-09-26' AND '2025-10-26'
    AND (
      ARRAY_CONTAINS('0xc2a0c36f5939a14966705c7cec813163faeea1f0'::variant, decoded_log:requiredDVNs)
      OR ARRAY_CONTAINS('0xc2a0c36f5939a14966705c7cec813163faeea1f0'::variant, decoded_log:optionalDVNs)
    )
),

base_oft AS (
  SELECT
    tx_hash                                   AS source_tx_hash,
    block_number                              AS source_block_number,
    block_timestamp                           AS source_timestamp,
    decoded_log:guid::string                  AS guid,
    decoded_log:nonce::number                 AS oft_nonce,
    decoded_log:dstEid::number                AS payload_dst_eid,
    decoded_log:srcEid::number                AS payload_src_eid,
    decoded_log:fromAddress::string           AS transaction_sender,
    decoded_log:amountSentLD::string          AS amount_sent_ld,
    decoded_log:encodedPayload::string        AS encoded_payload,
    '0x' || LOWER(SUBSTR(decoded_log:encodedPayload::string, 35, 40)) AS sender_address_hex,
    '0x' || LOWER(SUBSTR(decoded_log:encodedPayload::string, 99, 40)) AS receiver_address
  FROM base.core.ez_decoded_event_logs
  WHERE event_name = 'OFTSent'
    AND block_timestamp BETWEEN '2025-09-26' AND '2025-10-26'
),

eth_dest AS (
  SELECT
    tx_hash                                   AS dest_tx_hash,
    block_number                              AS dest_block_number,
    block_timestamp                           AS dest_timestamp,
    event_name                                AS dest_event_name,
    decoded_log:guid::string                  AS guid,
    decoded_log:origin:nonce::number          AS origin_nonce,
    decoded_log:origin:srcEid::number         AS origin_src_eid,
    decoded_log:origin:sender::string         AS origin_sender,
    decoded_log:payloadHash::string           AS payload_hash
  FROM ethereum.core.ez_decoded_event_logs
  WHERE event_name IN ('PacketDelivered','PacketVerified','OFTReceived')
    AND block_timestamp BETWEEN '2025-09-26' AND '2025-10-26'
),

executor_fee_paid AS (
  SELECT
    tx_hash AS executor_tx_hash,
    decoded_log:executor::string AS executor_address,
    decoded_log:fee AS executor_fee
  FROM base.core.ez_decoded_event_logs
  WHERE LOWER(contract_address) = LOWER('0xb5320b0b3a13cc860893e2bd79fcd7e13484dda2')
    AND event_name = 'ExecutorFeePaid'
    AND block_timestamp BETWEEN '2025-09-26' AND '2025-10-26'
)

SELECT
  b.source_tx_hash                                         AS SOURCETXHASH,
  REPLACE(TO_VARCHAR(b.source_block_number),',','')        AS SOURCEBLOCKNUMBER,
  b.source_timestamp                                       AS SOURCETIMESTAMP,
  '30184'                                                  AS SOURCEENDPOINTID,
  '30101'                                                  AS DESTINATIONENDPOINTID,
  CASE WHEN '30184' = '30184' THEN 'Base' ELSE 'Unknown' END AS SOURCE_CHAIN_NAME,
  CASE WHEN '30101' = '30101' THEN 'Ethereum' ELSE 'Unknown' END AS DEST_CHAIN_NAME,
  b.guid                                                   AS GUID,
  REPLACE(TO_VARCHAR(b.oft_nonce),',','')                  AS MESSAGENONCEDECIMAL,
  b.transaction_sender                                      AS TRANSACTIONSENDER,
  b.sender_address_hex                                      AS SENDERADDRESSHEX,
  b.receiver_address                                        AS RECEIVERADDRESS,
  b.encoded_payload                                        AS ENCODED_PAYLOAD,
  dt.required_dvns                                          AS REQUIREDDVNS,
  dt.optional_dvns                                          AS OPTIONALDVNS,
  REPLACE(TO_VARCHAR(ARRAY_SIZE(dt.required_dvns)),',','') AS REQUIREDDVNCOUNT,
  REPLACE(TO_VARCHAR(ARRAY_SIZE(dt.optional_dvns)),',','') AS OPTIONALDVNCOUNT,
  dt.dvn_tx_hash                                            AS DVNTXHASH,
  REPLACE(TO_VARCHAR(dt.dvn_block_number),',','')           AS DVNBLOCKNUMBER,
  dt.dvn_timestamp                                          AS DVNTIMESTAMP,
  dt.dvn_fees                                               AS DVN_FEES_ARRAY,
  ed.dest_tx_hash                                           AS DESTINATIONDELIVEREDTXHASH,
  REPLACE(TO_VARCHAR(ed.dest_block_number),',','')         AS DESTINATIONDELIVEREDBLOCKNUMBER,
  ed.dest_timestamp                                         AS DESTINATIONDELIVEREDTIMESTAMP,
  ed.dest_event_name                                        AS DEST_EVENT_NAME,
  ed.origin_nonce                                           AS DEST_ORIGIN_NONCE,
  ed.origin_src_eid                                         AS DEST_ORIGIN_SRCEID,
  ed.origin_sender                                          AS DEST_ORIGIN_SENDER,
  efp.executor_tx_hash                                      AS EXECUTOR_TXHASH,
  efp.executor_address                                      AS EXECUTORADDRESS,
  REPLACE(TO_VARCHAR(efp.executor_fee),',','')              AS EXECUTORFEE,
  CASE WHEN ed.dest_tx_hash IS NOT NULL THEN 'DELIVERED' ELSE 'SENT' END AS MESSAGESTATUS,
  CASE WHEN ed.dest_timestamp IS NOT NULL THEN REPLACE(TO_VARCHAR(DATEDIFF('second', b.source_timestamp, ed.dest_timestamp)),',','') ELSE 'N/A' END AS LATENCYTODELIVERY_SECONDS,
  CASE
    WHEN ed.guid IS NOT NULL AND b.guid = ed.guid THEN 'GUID'
    WHEN ed.origin_nonce = b.oft_nonce AND ed.origin_src_eid = 30184 THEN 'NONCE+SRC_EID'
    ELSE 'NO_MATCH'
  END AS MATCH_METHOD,
  (ARRAY_CONTAINS('0xc2a0c36f5939a14966705c7cec813163faeea1f0'::variant, dt.required_dvns)) AS DEUTSCHE_IS_REQUIRED,
  (ARRAY_CONTAINS('0xc2a0c36f5939a14966705c7cec813163faeea1f0'::variant, dt.optional_dvns)) AS DEUTSCHE_IS_OPTIONAL,
  CASE WHEN ed.dest_tx_hash IS NOT NULL THEN TRUE ELSE FALSE END AS DELIVERED_BOOL,
  REPLACE(TO_VARCHAR(b.source_block_number),',','') || '_' || REPLACE(TO_VARCHAR(ed.dest_block_number),',','') AS MESSAGE_PAIR_KEY,
  REPLACE(TO_VARCHAR(dt.dvn_block_number),',','') || '_' || REPLACE(TO_VARCHAR(b.source_block_number),',','') AS DVN_SOURCE_PAIR_KEY
FROM base_oft b
LEFT JOIN deutsche_telekom_dvn_txs dt
  ON b.source_tx_hash = dt.dvn_tx_hash
LEFT JOIN eth_dest ed
  ON (b.guid IS NOT NULL AND b.guid = ed.guid)
  OR (ed.origin_nonce = b.oft_nonce AND ed.origin_src_eid = 30184)
LEFT JOIN executor_fee_paid efp
  ON efp.executor_tx_hash = b.source_tx_hash
WHERE dt.dvn_tx_hash IS NOT NULL
ORDER BY b.source_block_number DESC
LIMIT 200;