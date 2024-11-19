import copy
import functools
import pathlib

import numpy as np
from attrs import (
    field,
    frozen,
)
from attrs.validators import (
    deep_iterable,
    instance_of,
)


(LSTM, ndf) = (None, None)


default_pdir = pathlib.Path("/mnt/nasa-data-download/data/Tail_666_1_parquet/")
default_t_names = ("ALT",)
base_x_names = (
    "time",
    "RALT",
    "PSA",
    "PI",
    "PT",
    "ALTR",
    "IVV",
    "VSPS",
    "FPAC",
    "BLAC",
    "CTAC",
    "TAS",
    "CAS",
    "GS",
    "CASS",
    "WS",
    "PTCH",
    "ROLL",
    "DA",
    "TAT",
    "SAT",
    "LATP",
    "LONP",
)


@frozen
class Config:
    n_rand_loops = field(validator=instance_of(int), default=3)
    n_files = field(validator=instance_of(int), default=3)
    batch_size = field(validator=instance_of(int), default=50_000)
    seq_length = field(validator=instance_of(int), default=8)
    l_rate = field(validator=instance_of(float), default=0.01)
    epoch = field(validator=instance_of(int), default=1000)
    n_h_unit = field(validator=instance_of(int), default=10)
    n_h_layer = field(validator=instance_of(int), default=1)
    pdir = field(validator=instance_of(pathlib.Path), converter=pathlib.Path, default=default_pdir)
    t_names = field(validator=deep_iterable(instance_of(str), instance_of(tuple)), default=default_t_names)
    x_names = field(validator=deep_iterable(instance_of(str), instance_of(tuple)), init=False)
    airborne_only = field(validator=instance_of(bool), default=True)
    vrtg = field(validator=instance_of(bool), default=True)
    train_frac = field(validator=instance_of(float), default=2/3)

    def __attrs_post_init__(self):
        if not self.pdir.exists():
            raise ValueError
        x_names = base_x_names
        if self.airborne_only or self.vrtg:
            x_names += ("LATG", "LONG", "VRTG")
        object.__setattr__(self, "x_names", x_names)

    @property
    def xlist(self):
        return list(self.x_names)

    @property
    def tlist(self):
        return list(self.t_names)

    @classmethod
    def get_debug_configs(cls):
        fixed = dict(
            n_rand_loops=1,
            n_files=3,
        )
        varying = dict(
            seq_length=(8,8),
            l_rate=(.01, .05),
            epoch=(150, 150),
            n_h_unit=(10, 10),
            n_h_layer=(1, 1),
        )
        yield from (
            cls(
                **dict(zip(varying.keys(), values)),
                **fixed,
            )
            for values in zip(*varying.values())
        )

    @classmethod
    def get_default_configs(cls):
        varying = dict(
            seq_length=(8, 8, 16, 16, 16, 16, 16, 32),
            l_rate=(0.01, 0.05, 0.01, 0.01, 0.03, 0.05, 0.05, 0.05),
            epoch=(1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000),
            n_h_unit=(10, 20, 10, 15, 20, 20, 25, 20),
            n_h_layer=(1, 1, 1, 2, 1, 1, 1, 1),
        )
        yield from (
            cls(**dict(zip(varying.keys(), values)))
            for values in zip(*varying.values())
        )

    @property
    @functools.cache
    def paths(self):
        return tuple(p for p in self.pdir.iterdir() if p.suffix == ".parquet")[:self.n_files]

    def get_train_paths(self, seed):
        # shuffle the files around in this list to get different results for the bootstrapping
        paths = shuffle(list(self.paths), seed)
        n_train = int(len(paths) * self.train_frac)
        train_paths = paths[:n_train]
        return train_paths

    def get_train_data(self, seed, **read_kwargs):
        train_paths = self.get_train_paths(seed=seed)
        X, T, scaleX, scaleT, time = ndf.read_filtered_scaled(
            train_paths,
            self.xlist,
            self.tlist,
            airborne_only=self.airborne_only,
            **read_kwargs,
        )
        return X, T, scaleX, scaleT, time

    def get_test_paths(self, seed):
        # shuffle the files around in this list to get different results for the bootstrapping
        paths = shuffle(list(self.paths), seed)
        n_train = int(len(paths) * self.train_frac)
        test_paths = paths[n_train:]
        return test_paths

    def get_test_data(self, seed, scaleX, scaleT, **read_kwargs):
        test_paths = self.get_test_paths(seed=seed)
        X, T, _, _, time = ndf.read_filtered_scaled(
            test_paths,
            self.xlist,
            self.tlist,
            scaleX=scaleX,
            scaleT=scaleT,
            airborne_only=self.airborne_only,
            **read_kwargs
        )
        return X, T, time

    def get_model(self):
        model = LSTM(
            len(self.x_names),
            len(self.t_names),
            self.n_h_unit,
            self.n_h_layer,
            self.device,
        )
        model = model.to(self.device)
        return model

    def train_model(self, seed, scaleX=None, scaleT=None, **read_kwargs):
        X, T, scaleX, scaleT, _ = self.get_train_data(seed=seed, **read_kwargs)
        model = self.get_model()
        loss_func = torch.nn.MSELoss()
        opt = torch.optim.Adam(model.parameters(), lr=self.l_rate)
        error_trace = []
        for _ in range(self.epoch):
            for _, (x, t) in enumerate(
                ndf.gen_batches(X, T, self.seq_length, batch_size=self.batch_size)
            ):
                # create tensors from the data frames
                Xtrain = torch.from_numpy(x.astype(np.float32)).to(self.device)
                Ttrain = torch.from_numpy(t.astype(np.float32)).to(self.device)

                ##run input forward through model
                train_output = model(Xtrain)

                # zero out gradients of optimized torch.Tensors to zero before back-propagation happens
                opt.zero_grad()

                # calculate the loss of the model output (Y) to the training Target (Ttrain)
                loss = loss_func(train_output, Ttrain)

                # back-propagation of the loss
                loss.backward()

                # performs a single optimization step (parameter update)
                opt.step()

                # keeping track of the history of the error in order to plot the convergence.
                error_trace.append(loss.detach().cpu())

        # do the last batch one last time
        loss = loss_func(train_output, Ttrain).detach().cpu()
        error_train = loss.item()
        torch.cuda.empty_cache()
        return (
            model,
            scaleX,
            scaleT,
            loss_func,
            opt,
            error_trace,
            error_train,
        )

    def test_model(self, seed, scaleX, scaleT, model, loss_func, **read_kwargs):
        X, T, _ = self.get_test_data(seed=seed, scaleX=scaleX, scaleT=scaleT, **read_kwargs)
        x, t, _ = map(np.array, zip(*ndf.gen_sliding_windows(X, T, None, self.seq_length)))

        # create tensors from the data frames
        Xtest = torch.from_numpy(x.astype(np.float32)).to(self.device)
        Ttest = torch.from_numpy(t.astype(np.float32)).to(self.device)

        # Run the test data through the trained model
        test_output = model(Xtest)
        # calculate the loss of the model output (Y) to the training Target (Ttrain)
        loss = loss_func(test_output, Ttest).detach().cpu()

        error_test = loss.item()
        torch.cuda.empty_cache()
        return error_test

    @property
    @functools.cache
    def device(self):
        if torch.cuda.is_available():
            (device, *_) = (torch.device(i) for i in range(torch.cuda.device_count()))
        else:
            device = torch.device("cpu")
            raise("we can't set_device to cpu")
        return device

    @property
    def to_kwargs(self):
        return {
            "device": self.device,
            # "dtype": torch.float32,
            # "non_blocking": False,
        }


def set_device(config):
    torch.cuda.set_device(config.device)


def print_cuda_settings():
    print(f"Total Number of CUDA Devices: {torch.cuda.device_count()}")
    print(
        f"Current CUDA Device: GPU{torch.cuda.current_device()} --> {torch.cuda.get_device_name()}"
    )
    print(f"Device Properties:\n  {torch.cuda.get_device_properties(torch.cuda.current_device())}")


def shuffle(x, seed=0):
    from numpy.random import Generator, PCG64
    rng = Generator(PCG64(seed))
    x = copy.copy(x)
    rng.shuffle(x)
    return x
