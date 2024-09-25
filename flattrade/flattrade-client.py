import asyncio
import websockets
import json
import aioconsole

# Global variables
websocket = None
running = False

async def connect(uri):
    global websocket
    while True:
        try:
            websocket = await websockets.connect(uri)
            print("Connected to WebSocket server")
            break
        except Exception as e:
            print(f"Connection failed: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)

async def subscribe(symbols):
    message = {
        "action": "subscribe",
        "symbols": symbols
    }
    await websocket.send(json.dumps(message))
    print(f"Subscribed to symbols: {symbols}")

async def unsubscribe(symbols):
    message = {
        "action": "unsubscribe",
        "symbols": symbols
    }
    await websocket.send(json.dumps(message))
    print(f"Unsubscribed from symbols: {symbols}")

async def receive_quotes():
    global running, websocket
    running = True
    while running:
        try:
            message = await websocket.recv()
            quote = json.loads(message)
            print(f"Received quote: {quote}")
            # Here you can process the quote data as needed
        except websockets.exceptions.ConnectionClosed:
            print("Connection closed, attempting to reconnect...")
            await connect(websocket.uri)
        except Exception as e:
            print(f"Error receiving quote: {e}")
            await asyncio.sleep(1)

async def stop():
    global running, websocket
    running = False
    await websocket.close()
    print("WebSocket connection closed")

async def user_input_handler():
    while True:
        command = await aioconsole.ainput("Enter command (subscribe/unsubscribe/quit): ")
        if command.lower() == 'quit':
            await stop()
            break
        elif command.lower() in ['subscribe', 'unsubscribe']:
            symbols = await aioconsole.ainput("Enter symbols (comma-separated): ")
            symbols_list = [s.strip() for s in symbols.split(',')]
            if command.lower() == 'subscribe':
                await subscribe(symbols_list)
            else:
                await unsubscribe(symbols_list)
        else:
            print("Invalid command. Please try again.")

async def other_operations():
    while True:
        print("Performing other operations...")
        await asyncio.sleep(10)  # Simulating other work every 10 seconds

async def main():
    uri = "ws://localhost:8765"
    await connect(uri)

    # Start receiving quotes
    quote_task = asyncio.create_task(receive_quotes())

    # Start user input handler
    input_task = asyncio.create_task(user_input_handler())

    # Start other operations
    other_task = asyncio.create_task(other_operations())

    # Wait for the user input handler to complete (i.e., user quits)
    await input_task

    # Cancel other tasks
    quote_task.cancel()
    other_task.cancel()

    print("Client shutting down...")

if __name__ == "__main__":
    asyncio.run(main())