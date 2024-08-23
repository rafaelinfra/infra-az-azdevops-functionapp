# Introduction 
- Para que funcione o CI/CD da forma que é esperada, é necessário utilizar o arquivo yaml que se encontra na pasta arquivos_CI. Deverá ser criado um pipeline a partir dele. O CD deverá ser realizado a partir de um artefato buildado no CI, e nas taks do pipeline utilizar a AzureFunctionApp@1, desta forma o CI/CD funcionará.

- durante o processo de CI foi construido um ambiente de teste para realizar antes do deploy, este teste se basea na function1 e na pasta Tests.
- Talvez necessário juntar CI/CD em um unico pipeline