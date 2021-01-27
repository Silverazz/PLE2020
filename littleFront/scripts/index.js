let userBtn = document.querySelector('body > div > form > button');
let userInput = document.querySelector('#user');

userBtn.addEventListener('click', event => {
    fetch('http://localhost:9090/test')
        .then(resp => {
            if(resp.ok) {
                console.log("TEST WORKING");
            }
            else{
                console.log(resp);
            }
        })
        .catch(error => {
            console.log("userBtn : "+ error);
        });
});


console.log('bouyaka');