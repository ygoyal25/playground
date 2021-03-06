const loader = document.getElementsByClassName('loader')[0];
function search() {
    console.log('Searching...');
    var tbl = document.getElementsByTagName('table')[0];
    var time_para = document.getElementsByClassName('time_para')[0];
    if (tbl) {
        tbl.remove();
    }

    if (time_para) {
        time_para.remove();
    }

    loader.setAttribute('style', 'display: block');
    const file = document.querySelector('#search').value;

    const fileType = [];
    const fileTypeOptions = document.querySelector('#fileType').options;
    for(var i = 0; i < fileTypeOptions.length; i++) {
        if (fileTypeOptions[i].selected) {
            fileType.push(fileTypeOptions[i].value);
        }    
    }

    const extensionOptions = document.querySelector('#extension').options;
    const extension = [];
    for(var i = 0; i < extensionOptions.length; i++) {
        if (extensionOptions[i].selected) {
            extension.push(extensionOptions[i].value);
        }    
    }

    const workingenvironmentidOptions = document.querySelector('#workingenvironmentid').options;
    const workingenvironmentid = [];
    for(var i = 0; i < workingenvironmentidOptions.length; i++) {
        if (workingenvironmentidOptions[i].selected) {
            workingenvironmentid.push(workingenvironmentidOptions[i].value);
        }    
    }


    console.log({
        file,
        fileType,
        extension,
        workingenvironmentid
    });

    let url = '/search?';

    if (file) {
        url += `file=${file}`;
    }

    if(fileType.length) {
        url += `&fileType=${fileType.join(',')}`
    }

    if (extension.length) {
        url += `&extension=${extension.join(',')}`
    }

    if (workingenvironmentid.length) {
        url += `&workingenvironmentid=${workingenvironmentid.join(',')}`
    }

    const startTime = new Date().getTime();
    let timeTaken;
    fetch(url)
    .then(data => {
        timeTaken = (new Date().getTime() - startTime) / 1000;
        return data.json()
    })
    .then(json => {        
        tableCreate(json.data, timeTaken);
    })
    .catch(e => {
        endTime = new Date().getTime();
        console.log(e);
    })
}

function tableCreate(data, timeTaken) {

    const p = document.createElement('p');
    p.setAttribute('class', 'time_para');
    p.innerText = `Fetched ${data.length} records in ${timeTaken} seconds`;

    loader.setAttribute('style', 'display: none');
    var body = document.getElementsByTagName('body')[0];
    body.appendChild(p);
    var tbl = document.createElement('table');
    tbl.setAttribute('class', 'table');
    tbl.style.width = '100%';
    tbl.setAttribute('border', '1');
    var thd = document.createElement('thead');
    var tbdy = document.createElement('tbody');

    var tr = document.createElement('tr');
    ['file', 'extension', 'fileType', 'changeType', 'size', 'workingEnvironmentId', 'volumeId', 'snapshotId']
    .forEach(item => {
        var th = document.createElement('th');
        th.setAttribute('scope', 'col');
        th.appendChild(document.createTextNode(item));
        tr.appendChild(th)
    });

    thd.appendChild(tr);

    for (var i = 0; i < data.length; i++) {
        const row = data[i];
        var tr = document.createElement('tr');
        // let totalCols = Object.keys(row).length;

        for(const col in row) {
            var td = document.createElement('td');
            td.setAttribute('scope', 'col');
            td.appendChild(document.createTextNode(row[col]))
            tr.appendChild(td)
        }

        // for (var j = 0; j < totalCols; j++) {
        // }
        tbdy.appendChild(tr);
    }
    tbl.appendChild(thd);
    tbl.appendChild(tbdy);
    body.appendChild(tbl)
}
  