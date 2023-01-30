import torch


# Cuda available?
print(torch.cuda.is_available())
# Cuda actually usable?
torch.Tensor([1, 2]).to('cuda')
