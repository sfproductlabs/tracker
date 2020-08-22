chrome.contextMenus.create({
  id: "tracker",
  title: "Track this URL",
  type: "normal",
  contexts: ["all"],  // ContextType
  onclick: function (word,tab) {
    var link = word.linkUrl || tab.url || word.selectionText;
    if (link) {      
      chrome.tabs.create({ url: "https://tr.sfpl.io/pub/v1/track_url.html?tu=1&url=" + encodeURIComponent(link) });
    } else {
      console.log("Couldn't determine link");
      alert("Couldn't determine link");
    }
  }
});

chrome.runtime.onInstalled.addListener(function () {
  chrome.storage.sync.set({ defaultUrl: 'http://sfproductlabs.com' }, function () {
    console.log('Set default Url.');
  });  
});


