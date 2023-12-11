import csv
import json
import asyncio
from datetime import datetime
from django.shortcuts import render
from django.http import HttpResponse
from .models import Candle
from django.db import transaction
from asgiref.sync import sync_to_async

# Convert candles to desired timeframe asynchronously
async def convert_timeframe(candles, timeframe):
    manipulated_data = []
    candles_count = len(candles)
    for i in range(0, candles_count, timeframe):
        subset = candles[i:i+timeframe] if i + timeframe <= candles_count else candles[i:candles_count]
        if subset:
            candle_data = {
                'instrument': subset[0].instrument,
                'date': subset[0].date,
                'time': subset[0].time,
                'open': subset[0].open,
                'high': max(candle.high for candle in subset),
                'low': min(candle.low for candle in subset),
                'close': subset[-1].close,
                'volume': sum(candle.volume for candle in subset)
            }
            manipulated_data.append(candle_data)
    return manipulated_data

# Function to create a candle asynchronously
@sync_to_async
def create_candle(**kwargs):
    return Candle.objects.create(**kwargs)

# Async view handling CSV upload
async def upload_csv(request):
    if request.method == 'POST' and request.FILES['csv_file']:
        csv_file = request.FILES['csv_file']
        candles = []
        invalid_volume_values = []
        
        # Process uploaded CSV file
        reader = csv.DictReader(csv_file.read().decode('utf-8').splitlines())
        print(reader.fieldnames)  # Check column names
        
        for row in reader:
            try:
                # Validate if the 'VOLUME' value is convertible to an integer before conversion
                volume_value = row['VOLUME']
                if volume_value.isdigit():  # Check if the value consists of digits
                    volume = int(volume_value)
                else:
                    # Handle the case where the value is not a valid integer
                    print(f"Invalid volume value: {volume_value}")
                    invalid_volume_values.append(volume_value)
                    continue  # Skip this row and proceed to the next

                candle = await create_candle(
                    instrument=row['BANKNIFTY'],
                    date=datetime.strptime(row['DATE'], '%Y%m%d'),
                    # time=datetime.strptime(row['TIME'], '%H:%M').time(),
                    time=row['TIME'],  # Store time as string
                    open=float(row['OPEN']),
                    high=float(row['HIGH']),
                    low=float(row['LOW']),
                    close=float(row['CLOSE']),
                    volume=volume
                )
                candles.append(candle)
            except KeyError as e:
                print(f"KeyError: {e}")

        # Convert candles to desired timeframe asynchronously
        timeframe = 10  # Timeframe in minutes
        manipulated_data = await convert_timeframe(candles, timeframe)

        # Convert manipulated data to JSON
        # manipulated_data_json = json.dumps(manipulated_data)
        manipulated_data_json = json.dumps([
            {
                k: v.strftime('%Y-%m-%d %H:%M:%S') if isinstance(v, datetime) else str(v)
                for k, v in candle_data.items()
            }
            for candle_data in manipulated_data
        ])

        # Save JSON data to a file (Replace 'path_to_file' with the desired path)
        with open('path_to_file/converted_data.json', 'w') as json_file:
            json_file.write(manipulated_data_json)

        # Provide the JSON file as a downloadable response
        response = HttpResponse(manipulated_data_json, content_type='application/json')
        response['Content-Disposition'] = 'attachment; filename="converted_data.json"'
        return response

    return render(request, 'upload.html')









# import csv
# import json
# import asyncio
# from datetime import datetime, timedelta
# from django.shortcuts import render
# from django.http import HttpResponse
# from .models import Candle
# from django.db import transaction
# from asgiref.sync import sync_to_async

# async def convert_timeframe(candles, timeframe):
#     manipulated_data = []
#     candles_count = len(candles)
#     for i in range(0, candles_count, timeframe):
#         if i + timeframe <= candles_count:
#             subset = candles[i:i+timeframe]
#         else:
#             subset = candles[i:candles_count]
#         if subset:
#             candle_data = {
#                 'instrument': subset[0].instrument,
#                 'date': subset[0].date,
#                 'time': subset[0].time,
#                 'open': subset[0].open,
#                 'high': max(candle.high for candle in subset),
#                 'low': min(candle.low for candle in subset),
#                 'close': subset[-1].close,
#                 'volume': sum(candle.volume for candle in subset)
#             }
#             manipulated_data.append(candle_data)
#     return manipulated_data




# async def upload_csv(request):
#     if request.method == 'POST' and request.FILES['csv_file']:
#         csv_file = request.FILES['csv_file']
#         candles = []
        
#         # Process uploaded CSV file
#         # reader = csv.DictReader(csv_file)
#         reader = csv.DictReader(csv_file.read().decode('utf-8').splitlines())
#         print(reader.fieldnames)  # Check column names
#         for row in reader:
#             @sync_to_async
#             def create_candle(**kwargs):
#                 return Candle.objects.create(**kwargs)
#             try:
#                 candle = Candle.objects.create(
#                     instrument=row['BANKNIFTY'],  # Replace with the correct column name
#                     date=datetime.strptime(row['DATE'], '%Y%m%d'),  # Replace with the correct column name and date format
#                     time=datetime.strptime(row['TIME'], '%H:%M').time(),  # Replace with the correct column name and time format
#                     open=float(row['OPEN']),  # Replace with the correct column name
#                     high=float(row['HIGH']),  # Replace with the correct column name
#                     low=float(row['LOW']),  # Replace with the correct column name
#                     close=float(row['CLOSE']),  # Replace with the correct column name
#                     volume=int(row['VOLUME'])  # Replace with the correct column name
#                 )
#                 candles.append(candle)
#             except KeyError as e:
#                 print(f"KeyError: {e}")


#         # Convert candles to desired timeframe asynchronously
#         timeframe = 10  # Timeframe in minutes
#         loop = asyncio.get_event_loop()
#         manipulated_data = await loop.run_in_executor(None, convert_timeframe, candles, timeframe)

#         # Convert manipulated data to JSON
#         manipulated_data_json = json.dumps(manipulated_data)

#         # Save JSON data to a file (Replace 'path_to_file' with the desired path)
#         with open('path_to_file/converted_data.json', 'w') as json_file:
#             json_file.write(manipulated_data_json)

#         # Provide the JSON file as a downloadable response
#         response = HttpResponse(manipulated_data_json, content_type='application/json')
#         response['Content-Disposition'] = 'attachment; filename="converted_data.json"'
#         return response

#     return render(request, 'upload.html')





# # Create your views here.
# import csv
# import json
# import asyncio
# from datetime import datetime, timedelta
# from django.shortcuts import render
# from django.http import HttpResponse
# from .models import Candle

# async def convert_timeframe(candles, timeframe):
#     manipulated_data = []
#     candles_count = len(candles)
#     for i in range(0, candles_count, timeframe):
#         if i + timeframe <= candles_count:
#             subset = candles[i:i+timeframe]
#         else:
#             subset = candles[i:candles_count]
#         if subset:
#             candle_data = {
#                 'instrument': subset[0].instrument,
#                 'date': subset[0].date,
#                 'time': subset[0].time,
#                 'open': subset[0].open,
#                 'high': max(candle.high for candle in subset),
#                 'low': min(candle.low for candle in subset),
#                 'close': subset[-1].close,
#                 'volume': sum(candle.volume for candle in subset)
#             }
#             manipulated_data.append(candle_data)
#     return manipulated_data

# async def upload_csv(request):
#     if request.method == 'POST' and request.FILES['csv_file']:
#         csv_file = request.FILES['csv_file']
#         candles = []
        
#         # Process uploaded CSV file
#         reader = csv.DictReader(csv_file)
#         for row in reader:
#             # Create Candle objects and save to database
#             candle = Candle.objects.create(
#                 instrument=row['BANKNIFTY'],
#                 date=datetime.strptime(row['DATE'], '%Y%m%d'),
#                 time=datetime.strptime(row['TIME'], '%H:%M').time(),
#                 open=float(row['OPEN']),
#                 high=float(row['HIGH']),
#                 low=float(row['LOW']),
#                 close=float(row['CLOSE']),
#                 volume=int(row['VOLUME'])
#             )
#             candles.append(candle)

#         # Convert candles to desired timeframe asynchronously
#         timeframe = 10  # Timeframe in minutes
#         loop = asyncio.get_event_loop()
#         manipulated_data = await loop.run_in_executor(None, convert_timeframe, candles, timeframe)

#         # Convert manipulated data to JSON
#         manipulated_data_json = json.dumps(manipulated_data)

#         # Save JSON data to a file (Replace 'path_to_file' with the desired path)
#         with open('path_to_file/converted_data.json', 'w') as json_file:
#             json_file.write(manipulated_data_json)

#         # Provide the JSON file as a downloadable response
#         response = HttpResponse(manipulated_data_json, content_type='application/json')
#         response['Content-Disposition'] = 'attachment; filename="converted_data.json"'
#         return response

#     return render(request, 'upload.html')
