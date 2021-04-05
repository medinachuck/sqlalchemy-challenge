import numpy as np
import pandas as pd
import datetime as dt
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify

# Create engine and reflect database into new model
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
Base = automap_base()
Base.prepare(engine, reflect=True)
session = Session(engine)

# Save references to tables
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create app to run
app = Flask(__name__)

# Define home route
@app.route('/')
def home():
    # Display welcome screen and list availalbe routes
    return '''THIS IS THE HOME PAGE!
    <br/>
    <br/>
    <br/>Available Routes: 
    <br/>
    <br/>/api/v1.0/precipitation
    <br/>/api/v1.0/stations
    <br/>/api/v1.0/tobs
    <br/>
    <br/>For the routes below, replace the words in CAPS with the beginning and/or end dates in this format: yyyy-mm-dd
    <br/>/api/v1.0/START DATE
    <br/>/api/v1.0/START DATE/END DATE'''

# Define route for precipitation data
@app.route('/api/v1.0/precipitation')
def precipitation():
    '''Fetch dates and precipitation data from database.'''
    # Query for dates and precipitation data and add them to a dictionary
    prcp_data = session.query(Measurement.date.label('date'), Measurement.prcp.label('prcp')).all()
    prcp_data = [row._asdict() for row in prcp_data]
    session.close()
    # Display jsonified results
    return jsonify(prcp_data)

# Define route for stations data 
@app.route('/api/v1.0/stations')
def stations():
    '''Fetch a list of stations from dataset.'''
    # Query for station names and unpack tuples into a list
    stations = session.query(Station.name).all()
    session.close()    
    stations = list(np.ravel(stations))
    # Display jsonified results
    return jsonify(stations)

# Define route for temperature data of station with most observations
@app.route('/api/v1.0/tobs')
def tobs():
    '''Fetch dates and temperature observations of the most active station for the last year of data.'''
    # Query for the last date that data was recorded for
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    last_date = [int(last_date[0][:4]), int(last_date[0][5:7]), int(last_date[0][8:])]
    year_ago = dt.date(last_date[0], last_date[1], last_date[2]) - dt.timedelta(days=365)
    session.close()
    # Query to join measurement and station tables, group them, and determine the number of observations each station has.
    stations = session.query(Measurement.station, Station.name, func.count(Measurement.station)).\
    filter(Measurement.station == Station.station).\
    group_by(Measurement.station).\
    order_by(func.count(Measurement.station).desc()).all()
    session.close()
    # Make a reference to the station with most observation data
    most_active_station = stations[0][0]
    # Query for the date and temperature data for the above station
    last_year_temps = session.query(Measurement.date, Measurement.tobs).\
    filter_by(station = most_active_station).\
    filter(Measurement.date > year_ago).all()
    session.close()
    # Display jsonified results
    return jsonify(last_year_temps)

# Define route for temperature summary data of chosen start date
@app.route('/api/v1.0/<start>')
def start_only(start):
    # Query for minimum temperature, average temperature, and max temperature of all dates after chosen start date
    summary_list = session.query(func.min(Measurement.tobs), func.round(func.avg(Measurement.tobs),2), func.max(Measurement.tobs)).\
    filter(Measurement.date >= start).first()
    session.close()
    # Display jsonified results
    return jsonify(summary_list)

# Define route for temperature summary data of chosen start and end date
@app.route('/api/v1.0/<start>/<end>')
def start_and_end(start, end):
    # Query for minimum temperature, average temperature, and max temperature of all dates between chosen start and end date
    summary_list = session.query(func.min(Measurement.tobs), func.round(func.avg(Measurement.tobs),2), func.max(Measurement.tobs)).\
    filter(Measurement.date.between(start, end)).first()
    session.close()
    # Display jsonified results
    return jsonify(summary_list)

if __name__ == '__main__':
    app.run(debug=True)