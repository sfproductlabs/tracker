import React, { useState, useRef, useEffect } from "react";
import ReactDOM, { createPortal } from "react-dom";
import track from "./lib/tracker/track"
//Facebook CAPI Info
//Manage https://business.facebook.com/events_manager2
//Events listed at https://business.facebook.com/business/help/402791146561655?id=1205376682832142
//Parameters listed at https://developers.facebook.com/docs/marketing-api/conversions-api/parameters
const App = () => {
  useEffect(() => {
    track({test_event_code : "TEST76022", ename: "TestEvent", fbx: true, cflags: 4096, action_source : "website", value: 34.4})
  }, [])
  
  return (
    <div className="App">
      <h1>Test Tracker</h1>
    </div>
  );
};

const rootElement = document.getElementById("root");
ReactDOM.render(<App />, rootElement);
