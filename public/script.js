const loader = document.getElementsByClassName('loader')[0];
function search() {
    console.log('Searching...');
    loader.setAttribute('style', 'display: block');
    const file = document.querySelector('#search').value;

    if (!file) {
        alert('Please Enter file Name');
        return
    }

    const fileType = document.querySelector('#fileType').value;
    const extension = document.querySelector('#extension').value;
    const workingenvironmentid = document.querySelector('#workingenvironmentid').value;

    console.log({
        file,
        fileType,
        extension,
        workingenvironmentid
    });

    let url = `/search?file=${file}`;

    if(fileType !== 'All') {
        url += `&fileType=${fileType}`
    }

    if (extension !== 'All') {
        url += `&extension=${extension}`
    }

    if (workingenvironmentid !== 'All') {
        url += `&workingenvironmentid=${workingenvironmentid}`
    }

    fetch(url)
    .then(data => {
        return data.json()
    })
    .then(json => {
        console.log(json);
        var tbl = document.getElementsByTagName('table')[0];
        if (tbl) {
            tbl.remove();
        }        
        tableCreate(json.data);
    })
    .catch(e => {
        console.log(e);
    })
}

function tableCreate(data) {
    loader.setAttribute('style', 'display: none');
    var body = document.getElementsByTagName('body')[0];
    var tbl = document.createElement('table');
    tbl.setAttribute('class', 'table');
    tbl.style.width = '100%';
    tbl.setAttribute('border', '1');
    var tbdy = document.createElement('tbody');
    for (var i = 0; i < data.length; i++) {
        const row = data[i];
        var tr = document.createElement('tr');
        // let totalCols = Object.keys(row).length;

        for(const col in row) {
            var td = document.createElement('td');
            td.appendChild(document.createTextNode(row[col]))
            tr.appendChild(td)
        }

        // for (var j = 0; j < totalCols; j++) {
        // }
        tbdy.appendChild(tr);
    }
    tbl.appendChild(tbdy);
    body.appendChild(tbl)
}
  