let btn1 = document.getElementById('btn1');

btn1.addEventListener('click', event => {
    fetch('http://localhost:8888/')
        .then(resp => {
            if(resp.ok) {
                console.log("TEST WORKING");
            }
            else{
                console.log(resp);
            }
        })
        .catch(error => {
            console.log("error cheh : "+ error);
        });
});

