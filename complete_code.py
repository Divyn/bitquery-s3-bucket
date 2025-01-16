import asyncio
import json
import websockets
import boto3

# AWS S3 configuration
s3_client = boto3.client(
    's3',
    aws_access_key_id='id',
    aws_secret_access_key='ss',
    region_name='ap-south-1'
)
bucket_name = 'pumpfun-bq'

# Bitquery WebSocket API details
url = "wss://streaming.bitquery.io/eap?token=ory_at_s20" 
query = """
subscription MyQuery {
  Solana {
    DEXTrades(
      where: {
        Trade: { Dex: { ProtocolName: { is: "pump" } } }
        Transaction: { Result: { Success: true } }
      }
    ) {
      Trade {
        Dex {
          ProtocolFamily
          ProtocolName
        }
        Buy {
          Amount
          Account {
            Address
          }
        }
        Sell {
          Amount
          Account {
            Address
          }
        }
      }
      Transaction {
        Signature
      }
    }
  }
}
"""

async def fetch_and_upload():
    async with websockets.connect(url, subprotocols=["graphql-ws"]) as websocket:
        # Step 1: Initialize connection
        await websocket.send(json.dumps({"type": "connection_init"}))
        
        # Wait for connection_ack
        while True:
            response = await websocket.recv()
            response_data = json.loads(response)
            if response_data.get("type") == "connection_ack":
                print("Connection acknowledged.")
                break

        # Step 2: Send subscription query
        await websocket.send(json.dumps({"type": "start", "id": "1", "payload": {"query": query}}))

        # Step 3: Listen and process messages
        while True:
            response = await websocket.recv()
            data = json.loads(response)
            
            # Process only subscription data
            if data.get("type") == "data" and "payload" in data:
                trades = data['payload']['data'].get('Solana', {}).get('DEXTrades', [])
                
                for trade in trades:
                    message = {
                        "protocol_family": trade['Trade']['Dex']['ProtocolFamily'],
                        "protocol_name": trade['Trade']['Dex']['ProtocolName'],
                        "buy_amount": trade['Trade']['Buy']['Amount'],
                        "buy_account": trade['Trade']['Buy']['Account']['Address'],
                        "sell_amount": trade['Trade']['Sell']['Amount'],
                        "sell_account": trade['Trade']['Sell']['Account']['Address'],
                        "transaction_signature": trade['Transaction']['Signature']
                    }
                    upload_to_s3(message)

def upload_to_s3(data):
    try:
        # Create a unique S3 key for each message
        s3_key = f"data/{data['transaction_signature']}.json"
        s3_client.put_object(Body=json.dumps(data), Bucket=bucket_name, Key=s3_key)
        print(f"Uploaded message to S3: {s3_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

async def main():
    try:
        await fetch_and_upload()
    except Exception as e:
        print(f"Error occurred: {e}")

# Run the main function
asyncio.run(main())
