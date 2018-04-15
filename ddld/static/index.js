(function () {
    'use strict';

    let API = {
        nodes (args) {
            const p = '/api/v1/nodes';
            if (!args) {
                return p;
            }
            if (typeof args === 'string' || args instanceof String) {
                return `${p}/${args}`;
            }
            return `${p}?${args.toString()}`;
        },

        cache (args) {
            const p = '/api/v1/cache';
            if (!args) {
                return p;
            }
            if (typeof args === 'string' || args instanceof String) {
                return `${p}/${args}`;
            }
            return `${p}?${args.toString()}`;
        },

        log () {
            return '/api/v1/log';
        },
    };


    function main () {
        return Promise.all([
            setupSearch(),
            setupDownload(),
            setupTrash(),
            setupDoCache(),
            setupSync(),
            setupCompare(),
            setupLogWatcher(),
        ]).then(_ => 0);
    }


    function setupSearch () {
        let input = document.querySelector('#search-input');
        let button = document.querySelector('#search-button');
        let result = document.querySelector('#search-result');

        input.addEventListener('keypress', (event) => {
            if (event.keyCode !== 10 && event.keyCode !== 13) {
                return;
            }
            onSubmitSearch(input.value, result);
        });
        button.addEventListener('click', (event) => {
            onSubmitSearch(input.value, result);
        });

        return Promise.resolve();
    }


    function onSubmitSearch (pattern, searchResultArea) {
        return search(pattern).then((data) => {
            return appendSearchResult(searchResultArea, data);
        });
    }


    function search (pattern) {
        let args = new URLSearchParams();
        args.set('pattern', pattern);
        let headers = new Headers();
        headers.set('Cache-Control', 'no-store');
        return fetch(API.nodes(args), {
          method: 'GET',
          headers: headers,
        }).then((response) => {
            return response.json();
        });
    }


    function appendSearchResult (searchResultArea, data) {
        data = createSearchResultList(data);
        searchResultArea.insertBefore(data, searchResultArea.firstElementChild);
    }


    function createSearchResultList (data) {
        let wrapper = document.createElement('div');
        wrapper.classList.add('search-group');

        wrapper.addEventListener('contextmenu', (event) => {
            event.preventDefault();
            let entries = wrapper.querySelectorAll('.search-entry');
            for (let entry of entries) {
                entry.classList.toggle('selected');
            }
        });

        if (data.length <= 0) {
            wrapper.classList.add('empty');
        } else {
            for (let result of data) {
                result = createSearchResult(result);
                wrapper.appendChild(result);
            }
        }

        return wrapper;
    }


    function createSearchResult (resultData) {
        let wrapper = document.createElement('div');
        wrapper.dataset.id = resultData.id;
        wrapper.classList.add('search-entry');

        let nameLabel = document.createElement('span');
        nameLabel.textContent = resultData.name;
        wrapper.appendChild(nameLabel);

        wrapper.addEventListener('click', (event) => {
            wrapper.classList.toggle('selected');
        });

        return wrapper;
    }


    function setupDownload () {
        let button = document.querySelector('#download-button');

        button.addEventListener('click', (event) => {
            let idList = getSelectedIDList();
            download(idList);
        });

        return Promise.resolve();
    }


    function download (idList) {
        let requests = idList.map((v) => {
            return fetch(API.cache(v), {
                method: 'PUT',
            });
        });
        return Promise.all(requests);
    }


    function setupTrash () {
        let button = document.querySelector('#trash-button');

        button.addEventListener('click', (event) => {
            let rv = confirm('trash?');
            if (!rv) {
                return;
            }
            let idList = getSelectedIDList();
            trash(idList);
        });

        return Promise.resolve();
    }


    function trash (idList) {
        let requests = idList.map((v) => {
            return fetch(API.nodes(v), {
                method: 'DELETE',
            });
        });
        return Promise.all(requests);
    }


    function setupDoCache () {
        let button = document.querySelector('#do-cache-button');

        button.addEventListener('click', (event) => {
            doCache();
        });

        return Promise.resolve();
    }


    function doCache () {
        let args = new URLSearchParams();
        args.append('paths[]', '/tmp');

        return fetch(API.cache(), {
            method: 'POST',
            body: args,
        }).then((response) => {
            return response.text();
        });
    }


    function setupSync () {
        let button = document.querySelector('#sync-button');

        button.addEventListener('click', (event) => {
            doSync();
        });

        return Promise.resolve();
    }


    function doSync () {
        return fetch(API.cache(), {
            method: 'POST',
        });
    }


    function setupCompare () {
        let button = document.querySelector('#compare-button');

        button.addEventListener('click', (event) => {
            let idList = getSelectedIDList();
            compare(idList);
        });

        return Promise.resolve();
    }


    async function compare (idList) {
        let args = new URLSearchParams();
        for (let id of idList) {
            args.append('nodes[]', id);
        }
        let rv = await fetch(API.cache(args), {
            method: 'GET',
        });
        rv = await rv.json();
        updateCompareResult(rv);
    }


    function updateCompareResult (result) {
        let block = document.querySelector('#compare-result');
        while (block.firstChild) {
            block.firstChild.remove();
        }
        if (!result) {
            block.textContent = 'OK';
            return;
        }
        for (let [size, path] of result) {
            let pre = document.createElement('pre');
            pre.textContent = `${size}: ${path}`;
            block.appendChild(pre);
        }
    }


    function getSelectedIDList () {
        let list = document.querySelectorAll('#search-result .selected');
        let idList = Array.prototype.map.call(list, (v) => {
            return v.dataset.id;
        });
        list.forEach((v) => {
            v.classList.remove('selected');
        });
        return idList;
    }


    function setupLogWatcher () {
      let result = document.querySelector('#log-watcher');

      setupLogSocket(result);

      let headers = new Headers();
      headers.set('Cache-Control', 'no-store');
      return fetch(API.log(), {
          method: 'GET',
          headers: headers,
      }).then((response) => {
          return response.json();
      }).then((logs) => {
          let msgs = logs.map(formatRecord);
          msgs.reverse();
          msgs.forEach((record) => {
              result.appendChild(record);
          });
      });
    }


    function setupLogSocket (resultBlock) {
        let ws = new WebSocket(`ws://${location.host}/api/v1/socket`);
        ws.addEventListener('message', (event) => {
            let record = formatRecord(JSON.parse(event.data));
            resultBlock.insertBefore(record, resultBlock.firstElementChild);
        });
        ws.addEventListener('close', (event) => {
            console.info('close', event);
            setupLogSocket(resultBlock);
        });
        ws.addEventListener('error', (event) => {
            console.info('error', event);
            setTimeout(() => {
                setupLogSocket(resultBlock);
            }, 5 * 1000);
        });
    }


    function formatRecord (record) {
        let a = document.createElement('pre');
        a.textContent = record.message;
        return a;
    }


    return main();

})();
