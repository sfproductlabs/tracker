import React, { useState, useRef, useEffect } from "react";
import ReactDOM, { createPortal } from "react-dom";
import track from "./lib/tracker/track"

const App = () => {
  useEffect(() => {
    track()
  }, [])
  
  return (
    <div className="App">
      <h1>Test Tracker</h1>
    </div>
  );
};

const rootElement = document.getElementById("root");
ReactDOM.render(<App />, rootElement);
