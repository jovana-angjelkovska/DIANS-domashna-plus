import React, { useEffect, useState } from 'react';
import './App.css';
import Plotly from 'plotly.js-dist';
import axios from 'axios';

function App() {
  const [financialData, setFinancialData] = useState([]);
  const [analyzedData, setAnalyzedData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selectedSymbol, setSelectedSymbol] = useState('AAPL'); // Default symbol
  const [symbols, setSymbols] = useState(['AAPL', 'MSFT', 'GOOGL', 'AMZN']); // List of symbols
  const [activeTab, setActiveTab] = useState('realTime'); // Tabs: 'realTime' or 'analyzedData'

  const API_KEY = 'G7YX5BO1DKR2NX00'; // Replace with your actual API key
  const TIME_INTERVAL = '5min'; // Can be '1min', '5min', '15min', etc.

  useEffect(() => {
    if (activeTab === 'realTime') {
      fetchDataFromAPI(selectedSymbol);
    } else if (activeTab === 'analyzedData') {
      fetchAnalyzedData();
    }
  }, [selectedSymbol, activeTab]);

  // Fetch real-time data from Alpha Vantage
  const fetchDataFromAPI = (symbol) => {
    setLoading(true);

    const url = `https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=${symbol}&interval=${TIME_INTERVAL}&apikey=${API_KEY}`;

    axios.get(url)
        .then(response => {
          const data = response.data['Time Series (5min)'];
          const formattedData = formatData(data);
          setFinancialData(formattedData);
          setLoading(false);
        })
        .catch(error => {
          console.error('Error fetching data from Alpha Vantage:', error);
          setLoading(false);
        });
  };

  // Simulate fetching analyzed data (e.g., from a backend service)
  const fetchAnalyzedData = () => {
    setLoading(true);

    // Simulated analyzed data (replace with actual API call)
    const simulatedData = [
      { date: '2023-10-01', trend: 150, prediction: 155 },
      { date: '2023-10-02', trend: 152, prediction: 157 },
      { date: '2023-10-03', trend: 153, prediction: 158 },
      { date: '2023-10-04', trend: 155, prediction: 160 },
      { date: '2023-10-05', trend: 157, prediction: 162 },
    ];

    setTimeout(() => {
      setAnalyzedData(simulatedData);
      setLoading(false);
    }, 1000); // Simulate network delay
  };

  const formatData = (data) => {
    return Object.keys(data).map(key => ({
      date: key,
      value: parseFloat(data[key]['4. close']),
    }));
  };

  // Render real-time data chart
  useEffect(() => {
    if (financialData.length > 0 && activeTab === 'realTime') {
      createPlotlyChart(financialData, selectedSymbol);
    }
  }, [financialData, selectedSymbol, activeTab]);

  // Render analyzed data chart
  useEffect(() => {
    if (analyzedData.length > 0 && activeTab === 'analyzedData') {
      createAnalyzedDataChart(analyzedData);
    }
  }, [analyzedData, activeTab]);

  const createPlotlyChart = (data, symbol) => {
    const trace1 = {
      x: data.map(d => d.date),
      y: data.map(d => d.value),
      type: 'scatter',
      mode: 'lines+markers',
      name: 'Stock Value',
    };

    const layout = {
      title: `${symbol} Stock Value Over Time (Real-Time Processing & Visualization)`,
      xaxis: {
        title: 'Date',
      },
      yaxis: {
        title: 'Value',
      },
    };

    Plotly.newPlot('plotly-chart', [trace1], layout);
  };

  const createAnalyzedDataChart = (data) => {
    const trace1 = {
      x: data.map(d => d.date),
      y: data.map(d => d.trend),
      type: 'scatter',
      mode: 'lines+markers',
      name: 'Trend',
    };

    const trace2 = {
      x: data.map(d => d.date),
      y: data.map(d => d.prediction),
      type: 'scatter',
      mode: 'lines+markers',
      name: 'Prediction',
    };

    const layout = {
      title: 'Analyzed Data: Trends and Predictions',
      xaxis: {
        title: 'Date',
      },
      yaxis: {
        title: 'Value',
      },
    };

    Plotly.newPlot('plotly-chart', [trace1, trace2], layout);
  };

  const handleSymbolChange = (event) => {
    setSelectedSymbol(event.target.value);
  };

  const handleTabChange = (tab) => {
    setActiveTab(tab);
  };

  return (
      <div className="App">
        {/* Navbar */}
        <div className="navbar">
          <h1>End-to-End Financial Data Solution</h1>
          <div>
            <select value={selectedSymbol} onChange={handleSymbolChange}>
              {symbols.map(symbol => (
                  <option key={symbol} value={symbol}>{symbol}</option>
              ))}
            </select>
            <button onClick={() => fetchDataFromAPI(selectedSymbol)}>Refresh Data</button>
          </div>
        </div>

        {/* Tabs */}
        <div className="tabs">
          <button
              className={activeTab === 'realTime' ? 'active' : ''}
              onClick={() => handleTabChange('realTime')}
          >
            Real-Time Data
          </button>
          <button
              className={activeTab === 'analyzedData' ? 'active' : ''}
              onClick={() => handleTabChange('analyzedData')}
          >
            Analyzed Data
          </button>
        </div>

        {/* Loading Message */}
        {loading ? (
            <p className="loading">Loading data...</p>
        ) : (
            <>
              {/* Real-Time Data Tab */}
              {activeTab === 'realTime' && (
                  <div className="chart-container">
                    <h2>Real-Time Financial Data Visualization</h2>
                    <div id="plotly-chart"></div>
                  </div>
              )}

              {/* Analyzed Data Tab */}
              {activeTab === 'analyzedData' && (
                  <div className="chart-container">
                    <h2>Analyzed Data Visualization</h2>
                    <div id="plotly-chart"></div>
                    <h3>Analyzed Data Table</h3>
                    <table>
                      <thead>
                      <tr>
                        <th>Date</th>
                        <th>Trend</th>
                        <th>Prediction</th>
                      </tr>
                      </thead>
                      <tbody>
                      {analyzedData.map((row, index) => (
                          <tr key={index}>
                            <td>{row.date}</td>
                            <td>{row.trend}</td>
                            <td>{row.prediction}</td>
                          </tr>
                      ))}
                      </tbody>
                    </table>
                  </div>
              )}
            </>
        )}

        {/* Footer */}
        <div className="footer">
          <p>
            <a href="https://github.com/yourusername" target="_blank" rel="noopener noreferrer">GitHub</a>
          </p>
          <p>
            Powered by React and Plotly | Data from <a href="https://www.alphavantage.co/" target="_blank" rel="noopener noreferrer">Alpha Vantage</a>
          </p>
        </div>
      </div>
  );
}

export default App;
