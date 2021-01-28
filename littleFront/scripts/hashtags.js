const table = document.querySelector('body > div > table > tbody');
const batchsize = 15;

let next = null;
let prev = null;

let generateTable = async (start) => {
    try {
        let response = await fetch(`http://localhost:9090/hashtags/${start}/${batchsize}`);
        if (response.ok) {
            let data = await response.json();
            for(lang of data){
                let str = "";
                for(c of lang.$)
                    str+=c.charCodeAt(0);
                table.appendChild(createRow(lang.key, str));
            }
            prev = start
            next = data.pop().key
        }
        else{
            alert("error catched");
        }
    } catch (error) {
        alert('Catch :'+error);
    }
}

let createRow = (username, nbTweet) => {
    let elt = document.createElement('tr');
    elt.innerHTML = `<th scope="row">${username}</th>\
                        <td>${nbTweet}</td>`;
    return elt;
}

let nextBtn = document.querySelector('body > div > button:nth-child(2)')

nextBtn.addEventListener('click', (event) => {
    table.innerHTML = ''
    generateTable(next)
})

let prevBtn = document.querySelector('body > div > button:nth-child(1)')

prevBtn.addEventListener('click', (event) => {
    table.innerHTML = ''
    generateTable(prev)
})

generateTable('*')