import opensemantic.core
import opensemantic.core.v1
import opensemantic.lab
import opensemantic.lab.v1


def test_opensemantic():

    # Create an instance of LaboratoryProcess
    model = opensemantic.lab.LaboratoryProcess(
        label=[opensemantic.core.Label(text="Test Entity")],
    )

    # Check if the instance is created successfully
    assert isinstance(
        model, opensemantic.lab.LaboratoryProcess
    ), "Failed to create an instance of LaboratoryProcess"

    # v1

    # Create an instance of LaboratoryProcess
    model = opensemantic.lab.v1.LaboratoryProcess(
        label=[opensemantic.core.v1.Label(text="Test Entity")],
    )

    # Check if the instance is created successfully
    assert isinstance(
        model, opensemantic.lab.v1.LaboratoryProcess
    ), "Failed to create an instance of LaboratoryProcess"


if __name__ == "__main__":
    test_opensemantic()
    print("All tests passed!")
