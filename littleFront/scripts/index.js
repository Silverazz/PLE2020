let inputUser = document.querySelector('body > div > input[type=text]');
let buttonUser = document.querySelector('body > div > button');

buttonUser.addEventListener('click', async event => {
    if(inputUser.value) {
        try {
            let resp = await fetch('http://localhost:9090/user/'+inputUser.value);
            // window.location.href = 'result.html';
        } catch (error) {
            alert(error);
        }
    }
});