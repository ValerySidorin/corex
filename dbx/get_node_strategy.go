package dbx

import "github.com/ValerySidorin/corex/dbx/cluster"

type GetNodeStragegy struct {
	Criteria cluster.NodeStateCriteria
	Wait     bool
}

func NoWaitAlive() GetNodeStragegy {
	return NoWait(cluster.Alive)
}

func WaitForAlive() GetNodeStragegy {
	return waitFor(cluster.Alive)
}

func NoWaitPrimary() GetNodeStragegy {
	return NoWait(cluster.Primary)
}

func WaitForPrimary() GetNodeStragegy {
	return waitFor(cluster.Primary)
}

func NoWaitPrimaryPreferred() GetNodeStragegy {
	return NoWait(cluster.PreferPrimary)
}

func WaitForPrimaryPreferred() GetNodeStragegy {
	return waitFor(cluster.PreferPrimary)
}

func NoWaitStandby() GetNodeStragegy {
	return NoWait(cluster.Standby)
}

func WaitForStandby() GetNodeStragegy {
	return waitFor(cluster.Standby)
}

func NoWaitStandbyPreferred() GetNodeStragegy {
	return NoWait(cluster.PreferStandby)
}

func WaitForStandbyPreferred() GetNodeStragegy {
	return waitFor(cluster.PreferStandby)
}

func NoWait(criteria cluster.NodeStateCriteria) GetNodeStragegy {
	return GetNodeStragegy{
		Criteria: criteria,
	}
}

func waitFor(criteria cluster.NodeStateCriteria) GetNodeStragegy {
	return GetNodeStragegy{
		Criteria: criteria,
		Wait:     true,
	}
}
