import React, { useEffect, useState } from 'react';
import './App.css';
import Plotly from 'plotly.js-dist';
import axios from 'axios';

function App() {
  const [financialData, setFinancialData] = useState([]);
  const [analyzedData, setAnalyzedData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selectedSymbol, setSelectedSymbol] = useState('AAPL'); 
  const [symbols, setSymbols] = useState(['AAPL', 'MSFT', 'GOOGL', 'AMZN']); 
  const [activeTab, setActiveTab] = useState('realTime'); 

  const API_KEY = 'G7YX5BO1DKR2NX00'; 
  const TIME_INTERVAL = '5min'; 

  useEffect(() => {
    if (activeTab === 'realTime') {
      fetchDataFromAPI(selectedSymbol);
    } else if (activeTab === 'analyzedData') {
      fetchAnalyzedData();
    }
  }, [selectedSymbol, activeTab]);

  
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

  const fetchAnalyzedData = () => {
    setLoading(true);
    fetch(`http://127.0.0.1:5001/api/analyzed-data/${selectedSymbol}`)
      .then((response) => response.json())
      .then((data) => {
        const formattedData = data.map((item) => ({
          ...item,
          date: new Date(item.date).toISOString().split("T")[0], 
        }));
        setAnalyzedData(formattedData); 
        setLoading(false);
      })
      .catch((error) => {
        console.error("Error fetching analyzed data:", error);
        setLoading(false);
      });
  };

  const formatData = (data) => {
    return Object.keys(data).map(key => ({
      date: key,
      value: parseFloat(data[key]['4. close']),
    }));
  };

  useEffect(() => {
    if (financialData.length > 0 && activeTab === 'realTime') {
      createPlotlyChart(financialData, selectedSymbol);
    }
  }, [financialData, selectedSymbol, activeTab]);

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
    const filteredData = data.filter(d => d.symbol === selectedSymbol && d.SMA_5 != null); // Filter by symbol
  
    const trace1 = {
      x: filteredData.map(d => new Date(d.date).toLocaleDateString()),
      y: filteredData.map(d => d.SMA_5), // Use SMA_5 as the trend
      type: 'scatter',
      mode: 'lines+markers',
      name: 'SMA_5',
      marker: { color: 'blue' }
    };
  
    const trace2 = {
      x: filteredData.map(d => new Date(d.date).toLocaleDateString()),
      y: filteredData.map(d => d.prediction),
      type: 'scatter',
      mode: 'lines+markers',
      name: 'Prediction',
      marker: { color: 'orange' }
    };
  
    const layout = {
      title: `${selectedSymbol} Analyzed Data: SMA_5 and Predictions`,
      xaxis: { title: 'Date' },
      yaxis: { title: 'Value' },
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
                  <th>Symbol</th>
                  <th>Latest Price</th>
                  <th>SMA 5</th>
                  <th>SMA 10</th>
                  <th>Predicted Next Close</th>
                  <th>Buy/Sell/Hold</th>
                  <th>Analysis Date</th>
                  </tr>
                </thead>
                <tbody>
                {analyzedData.map((row, index) => (
                  <tr key={index}>
                  <td>{row.symbol}</td>
                  <td>{row.latest_price}</td>
                  <td>{row.SMA_5}</td>
                  <td>{row.SMA_10}</td>
                  <td>{row.prediction}</td>  
                  <td>{row.recommendation}</td>  
                  <td>{row.date}</td>  
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
