let languages;

(async () => {
    try {
        let response = await fetch('http://localhost:9090/languages');
        if (response.ok) {
            let table = document.querySelector('body > div > table > tbody');
            let data = await response.json();
            for(lang of data){
                let str = "";
                for(c of lang.$)
                    str+=c.charCodeAt(0);
                table.appendChild(createRow(lang.key, str));
            }
        }
        else{
            alert("error catched");
        }
    } catch (error) {
        alert('Catch :'+error);
    }
})()

let createRow = (language, total) => {
    let elt = document.createElement('tr');
    elt.innerHTML = `<th scope="row">${language}</th>\
                        <td>${total}</td>`;
    return elt;
}
