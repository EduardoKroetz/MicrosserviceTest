
## ADRs

### Duplicar ou criar projeto compartilhado para as classes de eventos?

Decidi criar um projeto compartilhado (Shared.Contracts) referenciado via ProjectReference
pelos três serviços, contendo apenas os records de evento (tipos primitivos, sem lógica,
sem enums, sem entidades de domínio).

O acoplamento gerado é de build/deploy, não de runtime: o contrato real que trafega é o
JSON, e a desserialização já é tolerante a campos novos. O que o ProjectReference custa é
cadência de release independente — todos os serviços compilam contra a mesma versão do
contrato, então uma mudança tende a exigir recompilação e deploy conjunto, mesmo de
serviços que não usam o campo alterado.

A alternativa adotada em cenários reais normalmente não é duplicar, e sim distribuir os
contratos como pacote NuGet versionado: cada serviço fixa uma versão e decide quando
migrar. Duplicar as classes (cada consumidor declarando só os campos que usa) é o caminho
quando há múltiplas linguagens ou quando se quer contratos consumer-driven.

Para o escopo deste projeto (solo, três serviços, contratos estáveis), o ProjectReference
é suficiente e evita a cerimônia de manter um feed NuGet privado.