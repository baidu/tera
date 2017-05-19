# Tera Committer
为什么你需要成为tera的committer？因为你一旦成为了tera的committer，就意味着你的代码将有机会运行在一个实际的大规模生产环境中。同时，你的代码也能得到足够的应用场景验证，对实践大规模分布式编程，非常有帮助。因此期待你的加入！:-)


# 如何成为Tera Committer
作为一个tera的commiter，首要条件是需要对tera代码有足够的熟悉度。因此，除了熟读代码之外，要将tera真正run起来，这样能加深系统的理解。一旦发现问题，可以通过提issue和pull request，获得问题反馈和解决（当然，我们更欢迎自己解决问题的同学）。

提交issue时，如果是提交bug，需要描述复现方法、具体日志记录等；如果是新功能，则需要描述思路、附带设计文档，方便其他人review你的想法。

提交pull request时，需要按照一定的[规则](../en/contributor.md)填写commit log。

当你给tera开发了一到两个核心功能之后，就表明你对tera有了一定的把控能力，此时我们会将你列入Tera Committer的候选列表，经过内部讨论通过，就能正式成为tera的committer！:-)

# 如何进行Code Review
对他人的代码进行Code Review时，需要本着认真负责的原则，并按一定规则进行：

	1、每个pr惯例是需要得到两个LGTM，然后由最后一个LGTM的人执行merge；
	2、merge pr时，提pr的人，一般不merge自己的pr；
	3、code view时，避免两个都是新的commiter，完成LGTM就自行merge代码；
       4、push 代码时，尽量避免rebase，否则难以看出代码diff;
	5、merge代码时，需要将commit log进行合并，每个pr保证只有一个commint log。

	ps：每个commiter都应自觉遵守上述规则。
