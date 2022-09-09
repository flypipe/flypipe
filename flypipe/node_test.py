from flypipe.node import node


@node()
def a():
    return 2

@node()
def b():
    return 3


@node(inputs=[a, b])
def c(a, b):
    """Returns a + b"""
    return a + b


class TestNode:

    def test__name__(self):
        """The name of the function should be preserved after being decorated with Node"""
        assert c.__name__ == 'c'

    def test__doc__(self):
        assert c.__doc__ == 'Returns a + b'

    def test__call__(self):
        assert c(5, 6) == 11

    def test_run(self):
        assert c.run() == 5
